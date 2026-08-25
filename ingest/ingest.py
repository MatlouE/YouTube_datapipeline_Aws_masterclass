import pathlib
import kagglehub

# Ensure the data directory exists
output_dir = pathlib.Path("data")
output_dir.mkdir(parents=True, exist_ok=True)

# Download latest version into the data folder
path = kagglehub.dataset_download("datasnaek/youtube-new", output_dir=str(output_dir))
print("Path to dataset files:", path)