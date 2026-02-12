
import csv
import os
from collections import defaultdict

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
CSV_FILE = os.path.join(OUTPUT_DIR, 'llm_response_record.csv')
ACCURACY_FILE = os.path.join(OUTPUT_DIR, 'Accuracy.csv')

def calculate_model_accuracy_and_timing(csv_file=CSV_FILE, accuracy_file=ACCURACY_FILE):
    """
    Calculates accuracy and average execution time for each model using llm_response_record.csv.
    Writes results to Accuracy.csv.
    """
    model_correct = defaultdict(int)
    model_total = defaultdict(int)
    model_exec_times = defaultdict(list)
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            model = row['Model']
            analysis = row['Analysis'].strip().lower()
            majority = row['Majority'].strip().lower()
            exec_time = row.get('Exec Time (s)', row.get('ExecTimeSec', '')).strip()
            try:
                exec_time = float(exec_time)
            except Exception:
                exec_time = None
            if analysis == majority:
                model_correct[model] += 1
            model_total[model] += 1
            if exec_time is not None:
                model_exec_times[model].append(exec_time)
    with open(accuracy_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Model', 'Accuracy', 'Average Exec Time (s)', 'Total Predictions'])
        for model in sorted(model_total.keys()):
            total = model_total[model]
            correct = model_correct[model]
            accuracy = correct / total if total else 0
            avg_exec_time = sum(model_exec_times[model]) / len(model_exec_times[model]) if model_exec_times[model] else 0
            writer.writerow([
                model,
                f'{accuracy:.4f}',
                f'{avg_exec_time:.3f}',
                total
            ])
    print(f"Accuracy and timing stats written to {accuracy_file}")

def get_model_accuracy_dict(csv_file=CSV_FILE):
    """
    Returns a dictionary of model: accuracy (float) from llm_response_record.csv.
    """
    model_correct = defaultdict(int)
    model_total = defaultdict(int)
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            model = row['Model']
            analysis = row['Analysis'].strip().lower()
            majority = row['Majority'].strip().lower()
            if analysis == majority:
                model_correct[model] += 1
            model_total[model] += 1
    return {model: (model_correct[model] / model_total[model] if model_total[model] else 0)
            for model in model_total}
