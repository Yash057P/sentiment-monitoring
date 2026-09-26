# One-command setup for the project on Windows (keeps everything local to .venv).
#
#   powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
#
# It creates `.venv`, installs requirements, and downloads the portable
# runtimes Spark needs on Windows:
#   * a JRE 17        -> .venv\jvm17        (Spark 4.x requires JDK 17+)
#   * winutils.exe    -> .venv\hadoop_home  (Windows native helper for Hadoop)
#   * Kafka connector -> jars\              (Spark Structured Streaming <=> Kafka)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# ------------------------------------------------------------------ venv
Write-Host "==> Creating virtual environment (.venv)"
if (-not (Test-Path ".venv\Scripts\python.exe")) {
    python -m venv .venv
}
$py = ".venv\Scripts\python.exe"

Write-Host "==> Installing requirements into .venv"
& $py -m pip install --upgrade pip
& $py -m pip install -r requirements.txt

# ------------------------------------------------------------- portable JRE 17
$jreDir = ".venv\jvm17"
if (-not (Test-Path "$jreDir\bin\java.exe")) {
    Write-Host "==> Downloading portable Temurin JRE 17 (.venv\jvm17)"
    $zip = Join-Path $env:TEMP "jre17.zip"
    Invoke-WebRequest -Uri "https://api.adoptium.net/v3/binary/latest/17/ga/windows/x64/jre/hotspot/normal/eclipse" -OutFile $zip -UseBasicParsing
    Expand-Archive -LiteralPath $zip -DestinationPath ".venv" -Force
    $extracted = Get-ChildItem ".venv" -Directory | Where-Object { $_.Name -like "jdk-17*" } | Select-Object -First 1
    if ($extracted -and $extracted.FullName -ne (Resolve-Path $jreDir).Path) {
        Move-Item $extracted.FullName $jreDir -Force
    }
    Remove-Item $zip -Force -ErrorAction SilentlyContinue
} else {
    Write-Host "==> JRE 17 already present (.venv\jvm17)"
}

# ------------------------------------------------------------------ winutils
$hadoopHome = ".venv\hadoop_home"
if (-not (Test-Path "$hadoopHome\bin\winutils.exe")) {
    Write-Host "==> Downloading winutils.exe + hadoop.dll (.venv\hadoop_home)"
    New-Item -ItemType Directory -Force -Path "$hadoopHome\bin", "$hadoopHome\lib\native" | Out-Null
    $base = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin"
    Invoke-WebRequest -Uri "$base\winutils.exe" -OutFile "$hadoopHome\bin\winutils.exe" -UseBasicParsing
    Invoke-WebRequest -Uri "$base\hadoop.dll" -OutFile "$hadoopHome\bin\hadoop.dll" -UseBasicParsing
    # NativeCodeLoader looks for hadoop.dll under $HADOOP_HOME/lib/native
    Copy-Item "$hadoopHome\bin\hadoop.dll" "$hadoopHome\lib\native\hadoop.dll" -Force
} elseif (-not (Test-Path "$hadoopHome\lib\native\hadoop.dll")) {
    Write-Host "==> Adding hadoop.dll to lib\native (.venv\hadoop_home)"
    New-Item -ItemType Directory -Force -Path "$hadoopHome\lib\native" | Out-Null
    if (Test-Path "$hadoopHome\bin\hadoop.dll") {
        Copy-Item "$hadoopHome\bin\hadoop.dll" "$hadoopHome\lib\native\hadoop.dll" -Force
    }
} else {
    Write-Host "==> winutils already present (.venv\hadoop_home)"
}

# ----------------------------------------------------------------- kafka jars
if (-not (Get-ChildItem "jars\*.jar" -ErrorAction SilentlyContinue)) {
    Write-Host "==> Downloading Spark Kafka connector jars (jars\)"
    New-Item -ItemType Directory -Force -Path "jars" | Out-Null
    $base = "https://repo1.maven.org/maven2"
    $files = @(
        "org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.4/spark-sql-kafka-0-10_2.13-4.0.4.jar",
        "org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.4/spark-token-provider-kafka-0-10_2.13-4.0.4.jar",
        "org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar",
        "org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"
    )
    foreach ($f in $files) {
        $name = Split-Path $f -Leaf
        Invoke-WebRequest -Uri "$base/$f" -OutFile "jars\$name" -UseBasicParsing
    }
} else {
    Write-Host "==> Kafka connector jars already present (jars\)"
}

Write-Host ""
Write-Host "Setup complete."
Write-Host "1) Place the Sentiment140 CSV at data\training.1600000.processed.noemoticon.csv"
Write-Host "2) Start Kafka:        docker compose up -d"
Write-Host "3) Train the model:    .\.venv\Scripts\python.exe src\train_model.py"
Write-Host "4) Start streaming:    .\.venv\Scripts\python.exe src\streaming_sentiment.py"
Write-Host "5) Start producer:     .\.venv\Scripts\python.exe src\kafka_producer.py --messages-per-second 10"
Write-Host "6) Open the dashboard: .\.venv\Scripts\streamlit.exe run dashboard\app.py"