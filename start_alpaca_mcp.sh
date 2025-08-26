# clone repo
git clone https://github.com/alpacahq/alpaca-mcp-server.git
cd alpaca-mcp-server

# load .env into current shell
set -a
source .env
set +a

# build the docker image
docker build -t alpaca-mcp-server .

# run the server
docker run -it --rm \
  -e ALPACA_API_KEY="$ALPACA_API_KEY" \
  -e ALPACA_SECRET_KEY="$ALPACA_SECRET_KEY" \
  -p 8000:8000 \
  alpaca-mcp-server \
  --transport http --host 0.0.0.0 --port 8000
