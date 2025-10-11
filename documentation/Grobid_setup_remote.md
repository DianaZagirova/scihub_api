docker pull grobid/grobid:0.8.2-crf
docker run --rm --gpus all --init --ulimit core=0 -p 8070:8070 grobid/grobid:0.8.2-full