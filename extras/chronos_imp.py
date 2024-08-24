from chronos import ChronosPipeline
from utils.tools import StandardScaler
from pathlib import Path
from tqdm import tqdm
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
SAVE_FILE = os.getenv('SAVE_FILE', None)

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
for i in tqdm(range(len(df)-CONTEXT_LEN-PRED_LEN-1), "predicting: "):
    forecasts.append(pipeline.predict(
        context=torch.tensor(df.iloc[i:i+CONTEXT_LEN].to_numpy()).squeeze(),
        prediction_length=PRED_LEN,
        num_samples=1,
        temperature=TEMP,
        top_k=TOP_K,
        top_p=TOP_P).squeeze().numpy())

# calculate average loss
loss = 0
chart_data = []
for idx, forecast in enumerate(forecasts):
    # get actual data
    pred_start = idx+CONTEXT_LEN+1
    actual = df.iloc[pred_start:pred_start+PRED_LEN].to_numpy().squeeze()

    # scale the prediction and actual data
    scaler = StandardScaler()
    scaler.fit(np.concatenate([forecast, actual]))
    actual_scaled = scaler.transform(actual)
    forecast_scaled = scaler.transform(forecast)

    # store chart data
    chart_data.append((actual, forecast))

    # accumulate loss
    loss += torch.nn.functional.mse_loss(torch.tensor(actual_scaled), torch.tensor(forecast_scaled))
loss /= len(forecasts)
print(f"{MODEL=}, {NUM_DATA=}, {PRED_LEN=}, {CONTEXT_LEN=}, {TEMP=}, {TOP_K=}, {TOP_P=}")
print(loss)

if SAVE_FILE is not None:
    np.save(SAVE_FILE, np.array(forecasts))