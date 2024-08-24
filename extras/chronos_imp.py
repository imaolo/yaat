from chronos import ChronosPipeline
from utils.tools import StandardScaler
from pathlib import Path
import sys, os, gdown, torch, numpy as np, matplotlib.pyplot as plt, pandas as pd

# script parameters
MODEL = os.getenv('MODEL', 'tiny')
NUM_DATA = int(os.getenv('NUM_DATA', 30))
PRED_LEN = int(os.getenv('PRED_LEN', 12))
CONTEXT_LEN = int(os.getenv('CONTEXT_LEN', 12))
TEMP = os.getenv('TEMP', None)
TEMP = float(TEMP) if TEMP is not None else TEMP
TOP_K = os.getenv('TOP_K', None)
TOP_K = float(TOP_K) if TOP_K is not None else TOP_K
TOP_P = os.getenv('TOP_P', None)
TOP_P = float(TOP_P) if TOP_P is not None else TOP_P

# get the device
if torch.cuda.is_available(): device = 'cuda'
elif torch.backends.mps.is_available(): device='mps'
else: device = 'cpu'
print(f'{device=}')

# create the model
pipeline = ChronosPipeline.from_pretrained(
    "amazon/chronos-t5-"+MODEL,
    device_map=device,
    torch_dtype=torch.bfloat16,
)

# get the data
if not (p:=Path('../spy.csv')).exists():
    gdown.download('https://drive.google.com/uc?id=16V4ZDbx_dfGL4Ky1j1FWxYaWyxG6GGra', str(p), quiet=False)
df = pd.read_csv(str(p), nrows=NUM_DATA, usecols=['open'])

# predict!!
forecasts = []
for i in range(len(df)-CONTEXT_LEN-PRED_LEN-1):
    forecasts.append(pipeline.predict(
        context=torch.tensor(df.iloc[i:i+CONTEXT_LEN].to_numpy()).squeeze(),
        prediction_length=PRED_LEN,
        num_samples=1,
        temperature=TEMP,
        top_k=TOP_K,
        top_p=TOP_P).squeeze().numpy())

# calculate average loss
loss = 0
for idx, forecast in enumerate(forecasts):
    # get actual data
    pred_start = idx+CONTEXT_LEN+1
    actual = df.iloc[pred_start:pred_start+PRED_LEN].to_numpy().squeeze()

    # scale the prediction and actual data
    scaler = StandardScaler()
    scaler.fit(np.concatenate([forecast, actual]))
    actual = scaler.transform(actual)
    forecast = scaler.transform(forecast)

    # accumulate loss
    loss += torch.nn.functional.mse_loss(torch.tensor(forecast), torch.tensor(actual))
loss /= len(forecasts)
print(f"{MODEL=}, {NUM_DATA=}, {PRED_LEN=}, {CONTEXT_LEN=}, {TEMP=}, {TOP_K=}, {TOP_P=}")
print(loss)