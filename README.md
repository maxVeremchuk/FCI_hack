# Vision One Million grid-clean rebuild

This version rebuilds the city heat-grid generation so cells are clipped to city borders instead of spilling outside them.

Run:
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```
