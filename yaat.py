import random, math, torch, transformers, torch.nn as nn, matplotlib.pyplot as plt, matplotlib.image as mpimg
from util import get_tickers

# hyperparameters
batch_size = 8
block_size = 256
max_iters = 500
eval_int = 100
device = 'cuda' if torch.cuda.is_available() else 'cpu'
lr = 1e-4
eval_iters = 250
n_embd = 256
n_head = 6
n_layer = 6
dropout = 0.30
### ---------

# load data
tickers = get_tickers()

# process data
train = {sym: data[:n] for sym, data in tickers.items() if len(data[(n:=int(data.shape[0]*.9)):]) > block_size}
val = {sym: data[n:] for sym, data in tickers.items() if len(data[n:]) > block_size}
means = {sym: torch.mean(data) for sym, data in train.items()}
stds = {sym: torch.std(data) for sym, data in train.items()}

# helpers
def normalize(x, sym): return (x - means[sym]) / stds[sym]
def denormalize(x, sym): return (x * stds[sym]) + means[sym]
def get_batch(split='train'):
    data = train if split == 'train' else val
    data = data[sym:=random.choice(list(data.keys()))]
    ix = torch.randint(len(data) - block_size, (batch_size,))
    x = torch.stack([data[i:i+block_size] for i in ix]).to(device)
    y = torch.stack([data[i+1:i+block_size+1] for i in ix]).to(device)
    return normalize(x, sym), normalize(y, sym)

### define model ###

class FeedForward(nn.Module):
    def __init__(self, n_embd):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(n_embd, 4 * n_embd),
            nn.ReLU(),
            nn.Linear(4 * n_embd, n_embd),
            nn.Dropout(dropout),
        )
    def forward(self, x): return self.net(x)

class Head(nn.Module):
    def __init__(self, head_size):
        super().__init__()
        getlin = lambda: nn.Linear(n_embd, head_size, bias=False)
        self.key, self.query, self.value = getlin(), getlin(), getlin()
        self.register_buffer('tril', torch.tril(torch.ones(block_size, block_size)))
        self.dropout = nn.Dropout(dropout)
    
    def forward(self, x):
        B,T,C = x.shape

        wei = self.query(x) @ self.key(x).transpose(-2,-1) * C**-0.5
        wei = wei.masked_fill(self.tril[:T, :T] == 0, float('-inf'))
        return self.dropout(nn.functional.softmax(wei, dim=-1)) @ self.value(x)

class Attention(nn.Module):
    def __init__(self, num_heads, head_size):
        super().__init__()
        self.heads = nn.ModuleList([Head(head_size) for _ in range(num_heads)])
        self.proj = nn.Linear(num_heads * head_size, n_embd)
        self.dropout = nn.Dropout(dropout)
    def forward(self, x): return self.dropout(self.proj(torch.cat([head(x) for head in self.heads], dim=-1)))

class TransformerBlock(nn.Module):
    def __init__(self, n_embd, n_head):
        super().__init__()
        self.sa = Attention(n_head,  n_embd // n_head)
        self.ffwd = FeedForward(n_embd)
        self.ln1 = nn.LayerNorm(n_embd)
        self.ln2 = nn.LayerNorm(n_embd)
    def forward(self, x): return x+self.ffwd(self.ln2(x:=self.sa(self.ln1(x))))

class Transformer(nn.Module):
    def __init__(self):
        super().__init__()
        self.in_proj = nn.Linear(1, n_embd)
        self.wpe = nn.Embedding(block_size, n_embd)
        self.blocks = nn.Sequential(*[TransformerBlock(n_embd, n_head) for _ in range (n_layer)])
        self.ln = nn.LayerNorm(n_embd)
        self.lm_head = nn.Linear(n_embd, 1)

    def forward(self, idx, targets=None):
        B, T = idx.shape

        # add feature dimension
        idxemb = self.in_proj(idx.unsqueeze(-1))
        posemb = self.wpe(torch.arange(T, device=device).unsqueeze(0).repeat(B, 1))
        logits = self.lm_head(self.ln(self.blocks(idxemb + posemb)))
        if targets is None: return logits, None

        B, T, C = logits.shape
        return logits, nn.functional.mse_loss(logits.view(B*T, C), targets.view(B*T))

    def generate(self, idx:torch.Tensor, num_tokens:int):
        for _ in range(num_tokens):
            idx_ = idx[:, -block_size:]
            logits, loss = self(idx_)
            logit = logits[:, -1, :] # only one
            idx = torch.cat((idx, logit) , dim=1)
        return idx

# create model, optimizer, and lr
mdl = Transformer().to(device)
opt = torch.optim.AdamW(mdl.parameters(), lr=lr)
lr_sched = transformers.get_linear_schedule_with_warmup(opt, num_warmup_steps=1000, num_training_steps=max_iters)

# print the number of parameters in the model
print(sum(p.numel() for p in mdl.parameters())/1e6, 'M parameters')

# train helper
@torch.no_grad()
def estimate_loss(model):
    out = {}
    model.eval()
    for split in ['train', 'val']:
        losses = torch.zeros(eval_iters)
        for k in range(eval_iters):
            X, Y = get_batch(split)
            logits, loss = model(X, Y)
            losses[k] = loss.item()
        out[split] = losses.mean()
    model.train()
    return out

# train model
for iter in range(max_iters):
    if not iter % eval_int:
        losses = estimate_loss(mdl)
        print(f"step {iter}: train loss {losses['train']:.4f}, val loss {losses['val']:.4f}")

    xb, yb = get_batch('train')
    logits, loss = mdl(xb, yb)
    opt.zero_grad(set_to_none=True)
    loss.backward()
    opt.step()
    lr_sched.step()

# generate & plot
gen, cap = 100, 8
plotd = cap if (plotd:=math.ceil(math.sqrt(len(val)))) > (cap:=math.ceil(math.sqrt(cap))) else plotd
for i, (sym, data)in enumerate(val.items()):
    if len(data) < (end_idx:=block_size+gen) or i > plotd**2: continue
    # get actuals and predicted data
    ix = torch.randint(len(data) - (end_idx), (1,)).to(device)
    context = torch.stack([data[i:i+block_size] for i in ix]).to(device)
    preds = denormalize(mdl.generate(context, gen)[0][block_size:end_idx], sym).detach().cpu().numpy()
    act = tickers[sym][block_size:end_idx].detach().numpy()

    # add to plot
    x = torch.arange(len(preds)).detach().numpy()
    plt.subplot(plotd+1, plotd+1, i+1)
    plt.plot(x, act, label='Actual', color='blue')
    plt.plot(x, preds, label='Predicted', color='red')
    plt.legend()
    plt.title(sym)
plt.tight_layout()
plt.savefig('data/plot.png')