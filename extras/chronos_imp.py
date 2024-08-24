from chronos import ChronosPipeline
from utils.tools import StandardScaler
import sys, torch, numpy as np, matplotlib.pyplot as plt, pandas as pd

# create the model
if torch.cuda.is_available(): device = 'cuda'
elif torch.backends.mps.is_available(): device='mps'
else: device = 'cpu'
pipeline = ChronosPipeline.from_pretrained(
    "amazon/chronos-t5-tiny",
    device_map=device,
    torch_dtype=torch.bfloat16,
)

# get the data
df = pd.read_csv("../spy.csv")

# cut it down
df = df.head(30)

# define prediction parameters
NUM_SAMPLES = 1
PRED_LEN = 12

# predict!!
forecasts = [
    pipeline.predict(
        context=torch.tensor(df.iloc[i:i+PRED_LEN]["open"].to_numpy()),
        prediction_length=PRED_LEN,
        num_samples=NUM_SAMPLES).squeeze().numpy()
    for i in range(len(df)-PRED_LEN*2)]

# calculate average loss
loss = 0
for idx, forecast in enumerate(forecasts):
    # get actual data
    actual = df.iloc[idx+1+PRED_LEN:idx+1+PRED_LEN*2]['open'].to_numpy()

    # scale the prediction and actual data
    scaler = StandardScaler()
    scaler.fit(np.concatenate([forecast, actual]))
    actual = scaler.transform(actual)
    forecast = scaler.transform(forecast)

    # accumulate loss
    loss += torch.nn.functional.mse_loss(torch.tensor(forecast), torch.tensor(actual))
loss /= len(forecasts)
print(loss)