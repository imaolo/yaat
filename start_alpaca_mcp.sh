# load envs
set -a
source .env
set +a

# clone repo
git clone https://github.com/alpacahq/alpaca-mcp-server.git
cd alpaca-mcp-server

# build the docker image
docker build -t alpaca-mcp-server .

# run the server
docker run -it --rm \
  -e ALPACA_API_KEY="$ALPACA_API_KEY" \
  -e ALPACA_SECRET_KEY="$ALPACA_SECRET_KEY" \
  -p 8000:8000 \
  --entrypoint python \
  alpaca-mcp-server \
  /app/alpaca_mcp_server.py --transport http --host 0.0.0.0 --port 8000