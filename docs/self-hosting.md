# Self-hosting

[Back to overview](../README.md)

The server, Designer and web client are in `NodaLogicDesigner/`. Use Python 3.10 or newer and Git.

## Install

Linux / macOS:

```bash
git clone https://github.com/dvdocumentation/nodalogic.git
cd nodalogic/NodaLogicDesigner
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Windows (PowerShell):

```powershell
git clone https://github.com/dvdocumentation/nodalogic.git
cd nodalogic\NodaLogicDesigner
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Configure

Before starting the server, set your installation's values:

| File | Settings |
| --- | --- |
| `app.py` | `ADMIN_LOGIN`, `FLASK_SECRET`, S3 endpoint and bucket, and the S3 client's credentials. |
| `credentials.json` | Model endpoint, API key and model names for the AI features you use. |

## Run

From `NodaLogicDesigner/`, with the virtual environment activated:

```bash
python app.py
```

On Windows, use the environment's Python directly:

```powershell
.\.venv\Scripts\python.exe app.py
```

Open [http://localhost:5000](http://localhost:5000) in your browser and register an account using the administrator login configured in `app.py`.

## Load a configuration

Import a `.nod` file from [Samples](../Samples/) or create a configuration in Designer. Set its server addresses and, if used, Room aliases and exchange channels for your installation. Add the configuration to the web client or the Android app through its configuration repository or QR code.
