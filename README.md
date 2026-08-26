# SignalDeck Core

**SignalDeck Core** is the runtime component of the SignalDeck framework.

It provides:

- The Flask application runtime
- Configuration loading
- Processor orchestration
- Plugin discovery and blueprint registration
- CLI entrypoint (`signaldeck`)
- HTTP endpoints and page composition

It does **not** contain UI templates or processor implementations.

---

## Architecture Overview

SignalDeck consists of multiple layers:

Instance Config
↓
signaldeck-core
↓
signaldeck-sdk
↓
Plugins
↓
signaldeck-ui


- **signaldeck-core** → Runtime and orchestration  
- **signaldeck-sdk** → Processor contracts and base classes  
- **signaldeck-ui** → Layout, shared macros, static assets  
- **Plugins** → Processor implementations + component templates  
- **Instance repo** → Private configuration and deployment  

---

## Installation

```bash
pip install signaldeck-core
```

Entwicklung:
 ```
 py -m pip install -e . --config-settings editable_mode=compat
 ```


## Run application
```
signaldeck run --config config.json [--host 0.0.0.0] [--port 5000] [--debug] [--no-collect-data]
```

## Script repository

Scripts can be stored as individual JSON files in a configurable directory:

```json
{
  "cmd": {
    "scripts_path": "scripts"
  }
}
```

Relative paths are resolved relative to the main configuration file. If `scripts_path` is omitted, `scripts` is used.
Existing inline definitions under `cmd.script` remain supported; file-based scripts with the same name take precedence.

A script file is named `<script-name>.json` and contains the matching script name:

```json
{
  "name": "example",
  "variables": [
    {
      "name": "delay",
      "type": "float",
      "default": 5
    }
  ],
  "commands": [
    "echo Start",
    "sleep $delay",
    "echo Finished"
  ]
}
```

## Validate
```
signaldeck validate-config --config config/haus_demo.json
```


## List plugins
```
signaldeck list-plugins --config config/haus_demo.json

```

## Get config for processor
```
signaldeck get-config --processor signaldeck_plugin_main.processors.dummy_data.data.Data
```