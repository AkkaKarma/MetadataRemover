
# Metadata Remover

This tool monitors a specific folder and detects when there are new or modified files containing metadata (such as EXIF information in photos, author data in PDFs, etc.). When it finds this metadata, it analyzes it and sends notifications through a Telegram bot, showing what information has been found. It can also automatically clean these metadata using tools like ExifTool and QPDF if they are installed on the system. The script can operate in two modes: "watchdog" mode, which monitors changes in real time, or "schedule" mode, which checks the folder at regular intervals. It includes logging to record everything the program does and error handling to make it more robust.


## Installation

```bash
  git clone https://github.com/AkkaKarma/MetadataRemover
  cd MetadataRemover
  sudo apt upgrade && sudo apt update
  sudo apt-get install -y python3 exiftool qpdf
  pip3 install pillow requests schedule watchdog
  python3 met.py -h

```

