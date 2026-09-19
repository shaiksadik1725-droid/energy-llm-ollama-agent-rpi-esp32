# Local LLM Energy Monitoring Agent — Raspberry Pi + ESP32

An edge-AI energy monitoring project that combines electrical measurements, a local dashboard, and a locally hosted LLM agent.

## Overview

The project is designed around a Raspberry Pi / ESP32 architecture where electrical measurements can be monitored and interpreted locally. The Python agent processes measurements and provides human-readable status information, while the dashboard presents the system state in a browser.

## Main Features

- Local energy-monitoring workflow
- Electrical parameter analysis
- Browser dashboard
- Python-based monitoring agent
- Designed for Raspberry Pi + ESP32 integration
- Local LLM / Ollama-oriented architecture
- Warning and status generation from changing electrical values

## Technology Stack

- Python
- HTML / CSS / JavaScript
- Ollama / local LLM workflow
- Raspberry Pi
- ESP32

## Project Structure

```text
energy-llm-ollama-agent-rpi-esp32/
├── ollama_agent.py
└── dashboard.html
```

## System Concept

```text
Sensors / ESP32
      │
      ▼
Raspberry Pi
      │
      ├── Measurement processing
      ├── Local monitoring logic
      ├── LLM-assisted interpretation
      │
      ▼
Web Dashboard
```

## Why Local AI?

A local model can reduce dependence on cloud services and can keep the monitoring workflow closer to the edge device. This is useful for experiments involving privacy, offline operation, and low-latency automation.

## Future Improvements

- Add MQTT communication between ESP32 and Raspberry Pi
- Add persistent historical storage
- Add charts for voltage, current, power, frequency, and power factor
- Add alert acknowledgement and event logs
- Add model/tool-call safety boundaries for device control
- Add Docker-based deployment

## Author

**Sadik Shaik**

Computer Engineering / AI & Embedded Systems Projects
