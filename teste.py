with open('/opt/airflow/notebooks/validation_results.txt', 'r') as file:
    lines  = file.readline()
    last_line = lines[-1].strip()
if "Percentual de validações bem-sucedidas:" in first_line:
    percentage_str = first_line.split(":")[1].strip().replace('%', '')
    percentage = float(percentage_str)
            if percentage < 60.0:
        return 'invalido'
    else:
        return 'valido'