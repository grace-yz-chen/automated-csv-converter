import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
import subprocess
import os
import platform
import ctypes
import re

try:
    ctypes.windll.shcore.SetProcessDpiAwareness(1)
except Exception:
    pass

# Select CSV file dialog
def choose_file():
    file_path = filedialog.askopenfilename(
        title="Select CSV file",
        filetypes=[("CSV files", "*.csv")]
    )
    file_path_var.set(file_path)

# Click button "Convert"
def run_converter():
    file_path = file_path_var.get().strip()
    if not file_path:
        messagebox.showerror("Error", "Please select a CSV file first!")
        return
    if not file_path.lower().endswith(".csv"):
        messagebox.showerror("Error", "Please choose a file with .csv extension!")
        return

    log_text.delete(1.0, tk.END)
    log_text.insert(tk.END, "Starting conversion...\n\n")

    # Include header flag if header checkbox is unchecked
    header_flag = [] if has_header_var.get() else ["--no-header"]

    try:
        process = subprocess.Popen(
            ["python", "converter.py", file_path] + header_flag,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        for raw_line in process.stdout:
            # Remove color codes
            clean_line = re.sub(r'\x1B\[[0-9;]*m', '', raw_line)
            log_text.insert(tk.END, clean_line)
            log_text.see(tk.END)
        process.wait()

        if process.returncode == 0:
            log_text.insert(tk.END, "\nConversion completed successfully!\n")
        else:
            log_text.insert(tk.END, "\nConversion failed.\n")
        output_sql = os.path.splitext(file_path)[0] + ".sql"
        output_file_var.set(output_sql)
    except Exception as e:
        log_text.insert(tk.END, f"Error occurred: {e}\n")
        messagebox.showerror("Error", f"Error occurred: {e}")

def open_output_folder():
    """Open the containing folder of the CSV file (same as output file)."""
    file_path = file_path_var.get().strip()
    if not file_path or not os.path.exists(file_path):
        messagebox.showerror(
            "Error",
            "No file found. Please choose a CSV file and run the converter first!"
        )
        return

    folder = os.path.dirname(file_path)

    if platform.system() == "Windows":
        os.startfile(folder)
    elif platform.system() == "Darwin":
        subprocess.run(["open", folder])
    else:
        subprocess.run(["xdg-open", folder])

# Create main window
root = tk.Tk()
root.title("CSV to SQL Converter")

# Set initial desired window size
root.geometry("870x560")
root.update_idletasks()

# Get actual width and height after rendering
win_width = root.winfo_width()
win_height = root.winfo_height()

# Get screen size
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()

# Compute centered position
x = int((screen_width - win_width) / 2)
y = int((screen_height - win_height - 100) / 2)
root.geometry(f"{win_width}x{win_height}+{x}+{y}")

# Variables
file_path_var = tk.StringVar()
output_file_var = tk.StringVar()
has_header_var = tk.BooleanVar(value=True)

# Frame layout
frame = tk.Frame(root, padx=10, pady=10)
frame.pack()

# CSV file selection
tk.Label(frame, text="CSV file path:").grid(row=0, column=0, sticky="e")
entry = tk.Entry(frame, textvariable=file_path_var, width=50)
entry.grid(row=0, column=1)
tk.Button(frame, text="Browse...", command=choose_file).grid(row=0, column=2, padx=5)

# Header checkbox
tk.Checkbutton(
    frame,
    text="CSV file contains header",
    variable=has_header_var
).grid(row=1, column=0, columnspan=3, pady=2)

# Convert button
convert_btn = tk.Button(
    frame,
    text="Convert",
    command=run_converter,
    width=20
)
convert_btn.grid(row=2, column=0, columnspan=3, pady=10)

# Log area
log_text = scrolledtext.ScrolledText(frame, width=80, height=15)
log_text.grid(row=3, column=0, columnspan=3, pady=5)

# Open output folder button
open_btn = tk.Button(
    frame,
    text="Open Output Folder",
    command=open_output_folder,
    width=20
)
open_btn.grid(row=4, column=0, columnspan=3, pady=10)

root.mainloop()
