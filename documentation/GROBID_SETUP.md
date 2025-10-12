# Setting up GROBID Server

This guide explains how to set up and run the GROBID server, which is required for full text and metadata extraction from PDF papers.

working verison:
docker run --platform linux/amd64 --rm -e JAVA_OPTS="-Djdk.platform.useContainer=false" -p 8070:8070 grobid/grobid:0.8.2

## What is GROBID?

GROBID (GeneRation Of BIbliographic Data) is a machine learning library for extracting, parsing, and re-structuring raw documents such as PDF into structured XML/TEI encoded documents with a particular focus on technical and scientific publications.

## Installation Options

### Option 1: Using Docker (Recommended)

The easiest way to run GROBID is using Docker:

1. Install Docker on your system if you don't have it already:
   - [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)
   - [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)
   - [Docker for Linux](https://docs.docker.com/engine/install/)

2. Pull the GROBID Docker image:
   ```bash
   docker pull lfoppiano/grobid:0.7.3
   ```

3. Run the GROBID container:

   **For Intel/AMD (x86_64) systems:**
   ```bash
   docker run -t --rm -p 8070:8070 lfoppiano/grobid:0.7.3
   ```

   **For Apple Silicon (M1/M2/M3) Macs or other ARM64 systems:**
   ```bash
   docker run --platform linux/amd64 -t --rm -p 8070:8070 lfoppiano/grobid:0.7.3
   ```
   The `--platform linux/amd64` flag is necessary because GROBID Docker images are built for x86_64 architecture and need emulation on ARM64.

4. Verify that GROBID is running by opening http://localhost:8070 in your web browser.

#### Recommended Command for All Systems

The most reliable way to run GROBID is using the official image with the following command:

```bash
docker run \
  --rm \
  --init \
  --ulimit core=0 \
  -p 8070:8070 \
  grobid/grobid:0.8.2-crf
```

This command:
- Uses the official GROBID image with CRF models
- Automatically removes the container when it exits (`--rm`)
- Uses an init process to handle signals properly (`--init`)
- Prevents core dumps (`--ulimit core=0`)
- Maps port 8070 from the container to your host

#### Troubleshooting ARM64 Issues

If you encounter errors on Apple Silicon Macs (M1/M2/M3), try these alternatives:

1. Use the official image with platform specification:
   ```bash
   docker run --platform linux/amd64 --rm --init --ulimit core=0 -p 8070:8070 grobid/grobid:0.8.2-crf
   ```

2. Try with Java options to disable container detection:
   ```bash
   docker run --platform linux/amd64 -e JAVA_OPTS="-Djdk.platform.useContainer=false" --rm -p 8070:8070 grobid/grobid:0.7.2
   ```

3. Use an older version with JDK 11 instead of JDK 17:
   ```bash
   docker run --platform linux/amd64 --rm -p 8070:8070 lfoppiano/grobid:0.6.2
   ```

4. As a last resort, you can build GROBID locally on your ARM64 machine following the manual installation instructions below.

### Option 2: Manual Installation

If you prefer to install GROBID directly on your system:

1. Make sure you have Java 8+ installed on your system.

2. Download the latest GROBID version from the [releases page](https://github.com/kermitt2/grobid/releases).

3. Extract the archive:
   ```bash
   unzip grobid-[version].zip
   ```

4. Navigate to the GROBID directory:
   ```bash
   cd grobid-[version]
   ```

5. Run GROBID:
   ```bash
   ./gradlew run
   ```

6. Verify that GROBID is running by opening http://localhost:8070 in your web browser.

## Configuration

By default, the Sci-Hub GROBID Downloader expects the GROBID server to be running at `http://localhost:8070`. If you need to change this or other settings, edit the `config.json` file:

```json
{
  "grobid_server": "http://localhost:8070",
  "batch_size": 1000,
  "timeout": 180,
  "sleep_time": 5,
  "coordinates": [
    "title",
    "persName",
    "affiliation",
    "orgName",
    "formula",
    "figure",
    "ref",
    "biblStruct",
    "head",
    "p",
    "s",
    "note"
  ]
}
```

## Using GROBID with the Sci-Hub Downloader

Once GROBID is running, you can use the `scihub_grobid_downloader.py` script to download papers and process them with GROBID:

```bash
python scihub_grobid_downloader.py 10.1038/s41586-019-1750-x
```

Or process existing PDF files:

```bash
python scihub_grobid_downloader.py -p
```

## Offline Mode

If you don't have GROBID running, you can still use the basic functionality in offline mode:

```bash
python grobid_parser.py --pdf papers/10.1038_s41586-019-1750-x.pdf --offline
```

This will extract basic metadata from the filename but won't provide full text extraction.

## Troubleshooting

1. **Connection refused error**: Make sure the GROBID server is running and accessible at the configured URL.

2. **Timeout errors**: If you're processing large PDFs, try increasing the timeout value in `config.json`.

3. **Memory issues**: If GROBID crashes due to memory issues, you can allocate more memory when running the Docker container:
   ```bash
   docker run -t --rm -p 8070:8070 -e JAVA_OPTS="-Xmx4g" lfoppiano/grobid:0.7.3
   ```

4. **Processing errors**: Some PDFs may not be processed correctly due to their structure or quality. Try using a different PDF if possible.

## Additional Resources

- [GROBID Documentation](https://grobid.readthedocs.io/)
- [GROBID GitHub Repository](https://github.com/kermitt2/grobid)

# Start
docker run --rm --gpus all --init --ulimit core=0 -p 8070:8070 grobid/grobid:0.8.2-full