#!/usr/bin/env python3
"""Check if GROBID is using GPU."""

import requests
import subprocess
import sys

GROBID_URL = "http://10.223.131.158:8072"


def check_nvidia_available():
    """Check if NVIDIA GPU is available on the system."""
    print("Checking if NVIDIA GPU is available...")
    try:
        result = subprocess.run(['nvidia-smi'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            print("✓ NVIDIA GPU detected on system")
            print("\nGPU Info:")
            lines = result.stdout.split('\n')
            for line in lines[:15]:  # First 15 lines have GPU info
                if line.strip():
                    print(f"  {line}")
            return True
        else:
            print("✗ nvidia-smi failed")
            return False
    except FileNotFoundError:
        print("✗ nvidia-smi not found (No NVIDIA GPU or drivers not installed)")
        return False
    except Exception as e:
        print(f"✗ Error checking GPU: {e}")
        return False


def check_cuda_available():
    """Check if CUDA is available for Python."""
    print("\nChecking CUDA availability for Python...")
    try:
        import torch
        if torch.cuda.is_available():
            print(f"✓ CUDA is available")
            print(f"  CUDA version: {torch.version.cuda}")
            print(f"  GPU count: {torch.cuda.device_count()}")
            print(f"  GPU name: {torch.cuda.get_device_name(0)}")
            return True
        else:
            print("✗ CUDA not available in PyTorch")
            return False
    except ImportError:
        print("⚠ PyTorch not installed (cannot verify CUDA)")
        return None


def check_grobid_using_gpu():
    """Check if GROBID is configured to use GPU."""
    print("\nChecking GROBID GPU configuration...")
    
    # Method 1: Check GROBID logs for GPU indicators
    print("\n  Checking for GPU indicators in GROBID response...")
    try:
        # Try to get GROBID configuration endpoint (if available)
        r = requests.get(f"{GROBID_URL}/api/version", timeout=5)
        if r.status_code == 200:
            print(f"  ✓ GROBID responding: {r.text.strip()}")
    except:
        print("  ✗ Cannot query GROBID")
        return False
    
    # Method 2: Instructions to check manually
    print("\n  To verify GROBID is using GPU:")
    print("  1. Check GROBID logs:")
    print("     grep -i 'gpu\\|cuda\\|delft' /path/to/grobid/logs/grobid-service.log")
    print("")
    print("  2. Monitor GPU usage during processing:")
    print("     # Terminal 1:")
    print("     nvidia-smi -l 1")
    print("")
    print("     # Terminal 2:")
    print("     python parallel_download.py -f test_batch.txt -w 2 --parser grobid")
    print("")
    print("  3. Check GROBID config file:")
    print("     cat /path/to/grobid/grobid-home/config/grobid.yaml | grep -A 5 delft")
    
    return None


def provide_gpu_setup_instructions():
    """Provide instructions for enabling GPU."""
    print("\n" + "="*70)
    print("HOW TO ENABLE GPU IN GROBID")
    print("="*70)
    
    print("\n1. Install CUDA and cuDNN (if not already installed)")
    print("   - Download from: https://developer.nvidia.com/cuda-downloads")
    print("   - Required: CUDA 10.1 or later")
    
    print("\n2. Build GROBID with DeLFT (GPU support)")
    print("   cd /path/to/grobid")
    print("   ./gradlew clean assemble -Pdelft=true")
    
    print("\n3. Edit grobid.yaml to enable GPU:")
    print("   File: grobid/grobid-home/config/grobid.yaml")
    print("")
    print("   grobid:")
    print("     delft:")
    print("       enabled: true")
    print("       use_gpu: true")
    print("       gpu_device: 0")
    print("     concurrency: 8")
    print("     poolSize: 8")
    print("     jvmOptions: \"-Xmx8G\"")
    print("")
    print("   delft:")
    print("     device: \"cuda:0\"")
    print("     batch_size: 16")
    print("     use_amp: true")
    
    print("\n4. Restart GROBID")
    print("   cd /path/to/grobid")
    print("   ./gradlew run")
    
    print("\n5. Verify GPU usage")
    print("   # Watch GPU while processing")
    print("   nvidia-smi -l 1")
    print("")
    print("   # Process papers")
    print("   python parallel_download.py -f dois.txt -w 4 --parser grobid")
    print("")
    print("   # You should see GPU memory usage increase!")
    
    print("\nFor detailed instructions, see: GROBID_GPU_SETUP.md")


def main():
    print("="*70)
    print("GROBID GPU VERIFICATION")
    print("="*70)
    print()
    
    # Check 1: GPU available on system
    gpu_available = check_nvidia_available()
    
    # Check 2: CUDA available for Python
    cuda_available = check_cuda_available()
    
    # Check 3: GROBID GPU configuration
    grobid_gpu = check_grobid_using_gpu()
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    
    if not gpu_available:
        print("\n✗ No NVIDIA GPU detected on this system")
        print("  GROBID will run on CPU only")
        print("  Expected performance: ~8 papers/min with 4 workers")
    
    elif gpu_available and cuda_available:
        print("\n✓ GPU is available on this system")
        print("\n⚠ To use GPU with GROBID, you need to:")
        print("  1. Build GROBID with DeLFT support")
        print("  2. Configure grobid.yaml to enable GPU")
        print("  3. Restart GROBID server")
        provide_gpu_setup_instructions()
    
    elif gpu_available and cuda_available is None:
        print("\n✓ GPU is available on this system")
        print("⚠ PyTorch not installed - cannot fully verify CUDA")
        provide_gpu_setup_instructions()
    
    elif gpu_available:
        print("\n✓ GPU is available on this system")
        print("✗ CUDA not available in Python")
        print("\nInstall PyTorch with CUDA:")
        print("  conda install pytorch torchvision torchaudio pytorch-cuda=11.8 -c pytorch -c nvidia")
        provide_gpu_setup_instructions()
    
    print("\n" + "="*70)


if __name__ == "__main__":
    main()
