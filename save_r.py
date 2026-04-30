import os


def save_all_results(q1, q2, q3, q4, q5, q6):
    output_dir = "results"

    # Створюємо папку, якщо вона не існує
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Створено директорію для результатів: {output_dir}")

    print("\nЗапис результатів у CSV")

    # Словник для зручного обходу
    results = {
        "Q1": q1,
        "Q2": q2,
        "Q3": q3,
        "Q4": q4,
        "Q5": q5,
        "Q6": q6
    }

    for name, df in results.items():
        path = os.path.join(output_dir, name)
        # coalesce(1) використовується для об'єднання результатів у один CSV файл,
        # що зручно для невеликих агрегованих відповідей.
        df.coalesce(1).write.csv(path, header=True, mode="overwrite")
        print(f"Результат {name} збережено за шляхом: {path}")