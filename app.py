import tkinter as tk
import tkinter.font as tkfont  # Import tkinter.font for Font class
from tkinter import filedialog, messagebox, ttk
import threading
import time
import pandas as pd
from io import StringIO

from file_reader import read_file
from bats_logic import clean_bats_duplicates
from sales_logic import process_sales_data
from compare import compare_bats_sales

# Store data
bats_df = None               # raw BATS (cleaned on Start Process)
sales_raw_df = None          # raw Sales as loaded
sales_matched_df = None      # processed sales with Source & cleaned Deposit
leads_summary_df = None      # leads per source (from BATS)
comparison_df = None         # final per-Source GUI table

# ===== Paste helper =====
def read_pasted_data(raw_text):
    try:
        return pd.read_csv(
            StringIO(raw_text),
            sep=None,          # auto-detect delimiter
            engine="python",
            quotechar='"',
            on_bad_lines='skip'
        )
    except Exception as e:
        raise RuntimeError(f"Failed to parse pasted data: {e}")

# ===== Detect Header Row in BATS (specialized) =====
def detect_bats_skiprows(file_path):
    try:
        preview = pd.read_excel(file_path, header=None, nrows=5)
    except Exception as e:
        raise RuntimeError(f"Failed to read file for header detection: {e}")

    for i in range(len(preview)):
        row_values = preview.iloc[i].dropna().astype(str).str.lower()
        if any("phone" in v for v in row_values) and any("assigned" in v for v in row_values) and any("source" in v for v in row_values):
            return i
    return 0

# ===== GUI Table Display =====
def display_table(df, frame, table_name="Unknown"):
    for widget in frame.winfo_children():
        widget.destroy()

    if df is None or df.empty:
        tk.Label(frame, text="(no data)").pack()
        return

    # Limit to a subset of columns (e.g., first 15 or user-selected key columns)
    MAX_DISPLAY_COLUMNS = 15
    displayed_columns = df.columns[:MAX_DISPLAY_COLUMNS].tolist()
    if len(df.columns) > MAX_DISPLAY_COLUMNS:
        print(f"Warning: Table '{table_name}' has {len(df.columns)} columns, displaying only first {MAX_DISPLAY_COLUMNS}: {displayed_columns}")
    else:
        print(f"Table '{table_name}' has {len(df.columns)} columns: {displayed_columns}")

    # Container frame with fixed height and dynamic width
    tree_frame = tk.Frame(frame, height=150)
    tree_frame.pack(fill="both", expand=True)
    tree_frame.pack_propagate(False)

    # Treeview
    tree = ttk.Treeview(tree_frame, height=6)
    tree["columns"] = displayed_columns
    tree["show"] = "headings"

    # Set column width constraints
    MIN_COL_WIDTH = 80   # Minimum column width in pixels
    MAX_COL_WIDTH = 120  # Maximum column width in pixels
    DEFAULT_COL_WIDTH = 100  # Fallback width for invalid data

    # Calculate total available width for columns
    frame_width = frame.winfo_width() if frame.winfo_width() > 100 else 800
    num_columns = len(displayed_columns)
    target_col_width = max(MIN_COL_WIDTH, min(MAX_COL_WIDTH, frame_width // num_columns // 2))

    print(f"\n=== Displaying table '{table_name}' with {num_columns} columns, frame_width={frame_width}, target_col_width={target_col_width} ===")
    
    for col in displayed_columns:
        tree.heading(col, text=col)
        try:
            max_len = max(df[col].astype(str).map(len).max(), len(str(col)))
            col_width = min(max(max_len * 6, MIN_COL_WIDTH), MAX_COL_WIDTH)
            print(f"Column '{col}' in '{table_name}': max_len={max_len}, calculated_width={col_width}")
        except (ValueError, TypeError) as e:
            col_width = DEFAULT_COL_WIDTH
            print(f"Column '{col}' in '{table_name}': Error calculating max_len ({e}), using default_width={col_width}")
        tree.column(col, width=col_width, anchor="center", stretch=True)

    # Insert rows
    for _, row in df[displayed_columns].iterrows():
        values = []
        for val in row:
            val_str = str(val)
            if len(val_str) > 25:
                val_str = val_str[:22] + "..."
            values.append(val_str)
        tree.insert("", "end", values=values)

    # Scrollbars
    y_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
    x_scroll = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)

    tree.grid(row=0, column=0, sticky="nsew")
    y_scroll.grid(row=0, column=1, sticky="ns")
    x_scroll.grid(row=1, column=0, sticky="ew")

    tree_frame.grid_rowconfigure(0, weight=1)
    tree_frame.grid_columnconfigure(0, weight=1)

    # Dynamically adjust column widths to fit frame
    def update_treeview_width(event):
        current_frame_width = tree_frame.winfo_width()
        if current_frame_width <= 100:
            return
        total_col_width = sum(tree.column(col, "width") for col in displayed_columns)
        print(f"\n=== Resizing table '{table_name}': current_frame_width={current_frame_width}, total_col_width={total_col_width} ===")
        if total_col_width > current_frame_width:
            scale_factor = current_frame_width / total_col_width * 0.9
            for col in displayed_columns:
                new_width = max(MIN_COL_WIDTH, int(tree.column(col, "width") * scale_factor))
                print(f"Column '{col}' in '{table_name}': scaled_width={new_width}")
                tree.column(col, width=new_width)
        elif total_col_width < current_frame_width * 0.8:
            scale_factor = (current_frame_width * 0.8) / total_col_width
            for col in displayed_columns:
                new_width = min(MAX_COL_WIDTH, int(tree.column(col, "width") * scale_factor))
                print(f"Column '{col}' in '{table_name}': scaled_width={new_width}")
                tree.column(col, width=new_width)

    tree_frame.bind("<Configure>", update_treeview_width)    
    # ===== Processing =====
def start_process():
    global bats_df, leads_summary_df, sales_matched_df, comparison_df

    if bats_df is None:
        messagebox.showwarning("Warning", "Please upload the BATS file before processing.")
        return

    bats_progress["value"] = 0
    bats_progress.grid(row=1, column=0, sticky="ew", pady=5)

    try:
        bats_progress["value"] = 15
        root.update_idletasks()

        # Clean BATS duplicates + count leads per source
        cleaned_df, removed_count, leads_summary_df = clean_bats_duplicates(bats_df)
        bats_df = cleaned_df

        bats_progress["value"] = 40
        root.update_idletasks()
        display_table(bats_df, bats_table_frame)

        # If we already have Sales loaded, process them against the cleaned BATS now
        if sales_raw_df is not None:
            sales_matched_df, _sales_summary = process_sales_data(sales_raw_df, bats_df)
            display_table(sales_matched_df, sales_table_frame)

            # Build the final comparison (per Source)
            comparison_df = compare_bats_sales(bats_df, sales_matched_df)
            display_table(comparison_df, comparison_table_frame)

        bats_progress["value"] = 100
        root.update_idletasks()

        messagebox.showinfo(
            "Process Complete",
            f"Removed {removed_count} duplicate rows from BATS.\n"
            f"Sources counted: {len(leads_summary_df)}"
        )
        bats_progress.grid_forget()

    except Exception as e:
        bats_progress.grid_forget()
        messagebox.showerror("Error", f"Processing failed:\n{e}")

# ===== File Upload =====
def open_file(file_type):
    file_path = filedialog.askopenfilename(
        title=f"Select {file_type} file",
        filetypes=[("Excel files", "*.xlsx"), ("CSV files", "*.csv")]
    )
    if not file_path:
        return

    # Detect skiprows for BATS only
    skiprows = detect_bats_skiprows(file_path) if file_type == "Bats" and file_path.endswith(".xlsx") else 0

    target_frame = bats_table_frame if file_type == "Bats" else sales_table_frame
    progress_bar = bats_progress if file_type == "Bats" else sales_progress

    # Preview (first 30 rows)
    try:
        df_preview = read_file(file_path, skiprows=skiprows, preview_only=True)
        display_table(df_preview, target_frame)
    except Exception as e:
        messagebox.showerror("Error", f"Failed to read {file_type} preview:\n{e}")
        return

    # Load full file in background
    threading.Thread(
        target=load_full_file,
        args=(file_path, skiprows, target_frame, progress_bar, file_type),
        daemon=True
    ).start()

def load_full_file(file_path, skiprows, frame, progress_bar, file_type):
    global bats_df, sales_raw_df, sales_matched_df, comparison_df

    progress_bar["value"] = 0
    progress_bar.grid(row=1, column=0, sticky="ew", pady=5)

    for i in range(1, 50, 5):
        time.sleep(0.05)
        progress_bar["value"] = i

    try:
        df_full = read_file(file_path, skiprows=skiprows, preview_only=False)
        # Truncate long strings in the full dataset to match preview behavior
        for col in df_full.columns:
            df_full[col] = df_full[col].astype(str).apply(lambda x: x[:25] + "..." if len(str(x)) > 25 else x)
    except Exception as e:
        messagebox.showerror("Error", f"Failed to load full file:\n{e}")
        progress_bar.grid_forget()
        return

    progress_bar["value"] = 100
    time.sleep(0.2)
    progress_bar.grid_forget()

    if file_type == "Bats":
        bats_df = df_full
        display_table(bats_df, frame)
    else:
        sales_raw_df = df_full
        display_table(sales_raw_df, frame)

        if bats_df is not None:
            try:
                sales_matched_df, _ = process_sales_data(sales_raw_df, bats_df)
                display_table(sales_matched_df, sales_table_frame)
                comparison = compare_bats_sales(bats_df, sales_matched_df)
                display_table(comparison, comparison_table_frame)
            except Exception as e:
                messagebox.showerror("Error", f"Sales processing failed:\n{e}")

# ===== Paste Sales =====
def paste_sales_data():
    global sales_raw_df, sales_matched_df, comparison_df
    raw_text = sales_textbox.get("1.0", tk.END).strip()
    if not raw_text:
        messagebox.showwarning("Warning", "No data pasted.")
        return
    try:
        df = read_pasted_data(raw_text)
        sales_raw_df = df
        display_table(sales_raw_df, sales_table_frame)

        if bats_df is None:
            messagebox.showwarning("Warning", "Load BATS file first to attribute Source.")
            return

        sales_matched_df, _ = process_sales_data(sales_raw_df, bats_df)
        display_table(sales_matched_df, sales_table_frame)

        comparison = compare_bats_sales(bats_df, sales_matched_df)
        display_table(comparison, comparison_table_frame)

    except Exception as e:
        messagebox.showerror("Error", f"Failed to parse pasted data:\n{e}")

# ===== Export Sales Only =====
def export_to_excel():
    if sales_matched_df is None:
        messagebox.showwarning("Warning", "Please upload/paste Sales and click Start Process first.")
        return
    save_path = filedialog.asksaveasfilename(
        defaultextension=".xlsx",
        filetypes=[("Excel files", "*.xlsx")],
        title="Save Sales (matched) Data"
    )
    if not save_path:
        return
    try:
        sales_matched_df.to_excel(save_path, index=False)
        messagebox.showinfo("Success", f"Sales (matched) exported to:\n{save_path}")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to export Excel:\n{e}")

# ==== UI ====
root = tk.Tk()
root.title("BATS ↔ Sales Attribution")
root.geometry("1000x600")  # Smaller window to encourage scrolling

# Create a Canvas and Scrollbar
canvas = tk.Canvas(root)
scrollbar = ttk.Scrollbar(root, orient="vertical", command=canvas.yview)
canvas.configure(yscrollcommand=scrollbar.set)

# Create a frame inside the canvas to hold all widgets
main_frame = tk.Frame(canvas)
canvas.create_window((0, 0), window=main_frame, anchor="nw")

# BATS
bats_frame = tk.Frame(main_frame)
bats_frame.pack(fill="x", pady=10)

btn_bats = tk.Button(
    bats_frame, text="Upload BATS File",
    command=lambda: open_file("Bats"), bg="#d4f1f4",
    font=("Arial", 10, "bold"), relief="raised", padx=10, pady=5
)
btn_bats.grid(row=0, column=0, padx=10, pady=5, sticky="w")

process_btn = tk.Button(
    bats_frame, text="Start Process",
    command=start_process, bg="#ffeb99",
    font=("Arial", 10, "bold"), relief="raised", padx=10, pady=5
)
process_btn.grid(row=0, column=1, padx=10, pady=5, sticky="e")

bats_progress = ttk.Progressbar(bats_frame, orient="horizontal", mode="determinate", length=300)
bats_table_frame = tk.Frame(main_frame, height=150)
bats_table_frame.pack(fill="both", padx=10, pady=5)

# Sales
sales_frame = tk.Frame(main_frame)
sales_frame.pack(fill="x", pady=10)

btn_sales_upload = tk.Button(
    sales_frame, text="Upload Sales File",
    command=lambda: open_file("Sales"), bg="#f7d6d0",
    font=("Arial", 10, "bold"), relief="raised", padx=10, pady=5
)
btn_sales_upload.grid(row=0, column=0, padx=10, pady=5, sticky="w")

sales_progress = ttk.Progressbar(sales_frame, orient="horizontal", mode="determinate", length=300)

tk.Label(sales_frame, text="Or paste Sales data:", font=("Arial", 9, "italic")).grid(row=2, column=0, sticky="w", padx=10, pady=2)

# Create sales_textbox with pixel-based width and responsive sizing
sales_textbox = tk.Text(sales_frame, height=4, width=40)  # Reduced character width as starting point
sales_textbox.grid(row=3, column=0, padx=10, pady=5, sticky="ew")  # Use sticky="ew" for responsiveness
sales_frame.grid_columnconfigure(0, weight=1)  # Allow column to expand with frame

# Set maximum width in pixels using font metrics
font = tkfont.Font(family="Arial", size=10)  # Use tkfont.Font instead of tk.font.Font
char_width = font.measure("M")  # Approximate width of a character
max_textbox_width = 500  # Set maximum width in pixels (adjust as needed)
sales_textbox.config(width=int(max_textbox_width / char_width))  # Convert pixels to characters

# Dynamic resize handling for textbox
def resize_textbox(event):
    frame_width = sales_frame.winfo_width()
    if frame_width > 100:  # Only resize if frame is rendered
        new_width = min(max_textbox_width, frame_width - 20)  # Subtract padding
        sales_textbox.config(width=int(new_width / char_width))

sales_frame.bind("<Configure>", resize_textbox)

btn_paste_sales = tk.Button(
    sales_frame, text="Load Pasted Data",
    command=paste_sales_data, bg="#fce5cd",
    font=("Arial", 10, "bold"), relief="raised", padx=10, pady=5
)
btn_paste_sales.grid(row=4, column=0, padx=10, pady=5, sticky="w")

sales_table_frame = tk.Frame(main_frame, height=150)
sales_table_frame.pack(fill="both", padx=10, pady=5)

# Comparison
comparison_label = tk.Label(main_frame, text="Per-Source Summary", font=("Arial", 11, "bold"))
comparison_label.pack(pady=5)

comparison_table_frame = tk.Frame(main_frame, height=150)
comparison_table_frame.pack(fill="both", padx=10, pady=5)

# Export
export_btn = tk.Button(
    main_frame, text="Export Matched Sales to Excel",
    command=export_to_excel, bg="#c6efce",
    font=("Arial", 10, "bold"), relief="raised", padx=10, pady=5
)
export_btn.pack(pady=10)

# Pack canvas and scrollbar
canvas.pack(side="left", fill="both", expand=True)
scrollbar.pack(side="right", fill="y")

# Update scroll region when main_frame size changes
def configure_scroll_region(event):
    canvas.configure(scrollregion=canvas.bbox("all"))

main_frame.bind("<Configure>", configure_scroll_region)

# Enable mouse wheel scrolling
def on_mouse_wheel(event):
    canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

canvas.bind_all("<MouseWheel>", on_mouse_wheel)

root.mainloop()
# Close the Tkinter application
#root.destroy()