# HW01: Benchmarks, Report, Coverage

Все команды ниже запускаются из корня репозитория.

## 1) Запуск Python-скрипта бенчмарков

Скрипт: `hw01/tools/jmh_run_with_system_metrics.py`

### 1.1 Посмотреть список бенчмарков (без запуска)
```bash
python3 hw01/tools/jmh_run_with_system_metrics.py \
  --project-root . \
  --skip-build \
  --list-only
```

### 1.2 Полный запуск (дефолты из `@Warmup/@Measurement/@Fork` в Java-коде)
```bash
python3 hw01/tools/jmh_run_with_system_metrics.py \
  --project-root . \
  --out-dir hw01/target/jmh-system-report
```

### 1.3 Полный запуск с фильтром по benchmark
```bash
python3 hw01/tools/jmh_run_with_system_metrics.py \
  --project-root . \
  --skip-build \
  --out-dir hw01/target/jmh-system-report \
  --include 'bench\.ExtendableHashTableBenchmark\..*'
```

### 1.4 Полный запуск с override JMH параметров
```bash
python3 hw01/tools/jmh_run_with_system_metrics.py \
  --project-root . \
  --out-dir hw01/target/jmh-system-report \
  --jmh-args '-wi 8 -i 12 -f 2' \
  --sample-interval 1.0
```

### 1.5 Пересборка графиков и markdown из уже сохраненных JSON/CSV (без перезапуска JMH)
```bash
python3 hw01/tools/jmh_run_with_system_metrics.py \
  --project-root . \
  --out-dir hw01/target/jmh-system-report \
  --rebuild-only
```

## 2) Конвертация Markdown -> PDF

### 2.1 Установка инструментов (один раз)
```bash
brew install pandoc tectonic
```

### 2.2 Подготовить PDF-friendly markdown из `report.md`
```bash
python3 - <<'PY'
from pathlib import Path
import re

src = Path("hw01/target/jmh-system-report/report.md")
dst = Path("hw01/target/jmh-system-report/report_pdf.md")
text = src.read_text(encoding="utf-8")

def repl(m):
    path = m.group("src")
    alt = (m.group("alt") or "").strip() or "image"
    width = (m.group("width") or "").strip()
    if width == "920":
        w = "95%"
    elif width == "760":
        w = "78%"
    elif width:
        w = f"{width}px"
    else:
        w = "80%"
    return f"![{alt}]({path}){{ width={w} }}"

pattern = re.compile(r'<img\s+src="(?P<src>[^"]+)"(?:\s+alt="(?P<alt>[^"]*)")?(?:\s+width="(?P<width>[^"]+)")?\s*/?>')
dst.write_text(pattern.sub(repl, text), encoding="utf-8")
print(dst)
PY
```

### 2.3 Создать header для pandoc (переносы/таблицы)
```bash
cat > hw01/target/jmh-system-report/pandoc-header.tex <<'EOF'
\usepackage{xurl}
\urlstyle{same}
\setlength{\emergencystretch}{3em}
\sloppy
\usepackage{etoolbox}
\AtBeginEnvironment{longtable}{\footnotesize}
EOF
```

### 2.4 Собрать PDF
```bash
cd hw01/target/jmh-system-report
pandoc report_pdf.md -o report-best.pdf \
  --pdf-engine=tectonic \
  -V mainfont='Arial Unicode MS' \
  -V monofont='Menlo' \
  -V geometry:margin=1.6cm \
  -V fontsize=10pt \
  --include-in-header=pandoc-header.tex
```

Итоговый файл: `hw01/target/jmh-system-report/report-best.pdf`

## 3) JaCoCo: покрытие тестами

### 3.1 Запустить unit-тесты с JaCoCo
```bash
mvn -pl hw01 test
```

### 3.2 Где смотреть отчеты
- HTML: `hw01/target/site/jacoco-ut/index.html`
- XML: `hw01/target/site/jacoco-ut/jacoco.xml`
- Exec: `hw01/target/jacoco-ut.exec`

Быстро открыть HTML-отчет на macOS:
```bash
open hw01/target/site/jacoco-ut/index.html
```
