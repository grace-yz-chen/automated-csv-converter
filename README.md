# Automated CSV Converter

The **Automated CSV-to-PostgreSQL Converter** parses CSV files into the corresponding PostgreSQL **DDL** and **DML** statements.  
It accurately detects data types (including mixed-type columns) and handles missing values.  

The tool provides both a **command-line version** and a **GUI desktop version**.  
Source code includes:

- `converter.py`
- `gui.py`
- `utils.py`

⚠️ These files must be placed in the same folder.

---

## 🖥️ Environment

- Operating System: **Windows** / **Ubuntu (Linux)**  
- Python: **3.8+** must be installed  

---

## 📦 Python Dependencies

Install required libraries:

```bash
pip install pandas numpy python-dateutil shapely
