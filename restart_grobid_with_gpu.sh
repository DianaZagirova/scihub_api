#!/bin/bash
# Restart GROBID Docker container with GPU support

echo "========================================"
echo "GROBID Docker GPU Setup"
echo "========================================"
echo ""

# Check if nvidia-docker is installed
echo "1. Checking nvidia-docker..."
if docker run --rm --gpus all nvidia/cuda:11.8.0-base-ubuntu22.04 nvidia-smi &>/dev/null; then
    echo "   ✓ nvidia-docker is working"
else
    echo "   ✗ nvidia-docker not working"
    echo ""
    echo "   Install with:"
    echo "   sudo apt-get install -y nvidia-docker2"
    echo "   sudo systemctl restart docker"
    exit 1
fi

# Find current GROBID containers
echo ""
echo "2. Checking for existing GROBID containers..."
GROBID_CONTAINERS=$(docker ps -a --filter "ancestor=grobid/grobid" --format "{{.Names}}" 2>/dev/null)

if [ -n "$GROBID_CONTAINERS" ]; then
    echo "   Found existing containers:"
    echo "$GROBID_CONTAINERS" | while read container; do
        echo "     - $container"
    done
    echo ""
    echo "   Stopping them..."
    docker stop $GROBID_CONTAINERS 2>/dev/null
    echo "   ✓ Stopped"
else
    echo "   No existing GROBID containers found"
fi

# Check GPU availability
echo ""
echo "3. Checking GPU availability..."
nvidia-smi --query-gpu=index,name,memory.used,memory.total --format=csv,noheader | while read line; do
    echo "   GPU $line"
done

# Recommend which GPU to use
echo ""
echo "4. Selecting GPU..."
# Get GPU with most free memory
BEST_GPU=$(nvidia-smi --query-gpu=index,memory.free --format=csv,noheader,nounits | sort -k2 -nr | head -1 | cut -d',' -f1)
echo "   Recommended GPU: $BEST_GPU (most free memory)"

# Start new container with GPU
echo ""
echo "5. Starting GROBID with GPU support..."
echo ""

CONTAINER_NAME="grobid-gpu"
PORT="8072"

# The command to run
CMD="docker run -d --name $CONTAINER_NAME \
  --gpus '\"device=$BEST_GPU\"' \
  -p $PORT:8070 \
  -e GROBID_USE_GPU=true \
  -e GROBID_DELFT_ENABLED=true \
  -e CUDA_VISIBLE_DEVICES=$BEST_GPU \
  grobid/grobid:0.8.2"

echo "   Running:"
echo "   $CMD"
echo ""

# Check if image exists, if not pull it
if ! docker images | grep -q "grobid/grobid.*0.8.2"; then
    echo "   Pulling GROBID image..."
    docker pull grobid/grobid:0.8.2
fi

# Run the container
eval $CMD

if [ $? -eq 0 ]; then
    echo ""
    echo "   ✓ GROBID started successfully!"
    echo ""
    echo "   Container: $CONTAINER_NAME"
    echo "   GPU: $BEST_GPU"
    echo "   Port: $PORT"
    echo "   URL: http://10.223.131.158:$PORT"
else
    echo ""
    echo "   ✗ Failed to start GROBID"
    exit 1
fi

# Wait for GROBID to start
echo ""
echo "6. Waiting for GROBID to start..."
for i in {1..30}; do
    if curl -s http://localhost:$PORT/api/isalive &>/dev/null; then
        echo "   ✓ GROBID is ready!"
        break
    fi
    echo "   Waiting... ($i/30)"
    sleep 2
done

# Verify GPU access
echo ""
echo "7. Verifying GPU access in container..."
if docker exec $CONTAINER_NAME nvidia-smi &>/dev/null; then
    echo "   ✓ Container has GPU access"
    docker exec $CONTAINER_NAME nvidia-smi --query-gpu=name,memory.total --format=csv,noheader
else
    echo "   ✗ Container cannot access GPU"
    echo "   Check nvidia-docker installation"
fi

# Show next steps
echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo "1. Test GPU usage:"
echo "   ./test_grobid_gpu_precise.sh"
echo ""
echo "2. Monitor GPU while processing:"
echo "   watch -n 1 nvidia-smi"
echo ""
echo "3. Process papers with GPU:"
echo "   python parallel_download.py -f dois.txt -w 4 --parser grobid"
echo ""
echo "To view logs:"
echo "   docker logs -f $CONTAINER_NAME"
echo ""
