#!/bin/bash
set -e

bandirconfig="$1"
folder="$2"
safetyAPIkey="$3"

# Run black
pip install black
echo "Run black..."
python -m black --check ${folder}

# Run bandit test
pip install bandit
echo "Run bandit..." 
bandit -c ${bandirconfig} -r ${folder}

startfolder=$PWD
for file in $( find ${folder} -type f -name "pyproject.toml" ); do
	echo "Vulnerability check $file"
  echo ${safetyAPIkey}
	(
	  pip install poetry==1.4.2
	  cd $(dirname $(realpath $file))
    poetry install
    pip uninstall -y poetry && pip install safety
    # Ignore cleo vuln until a fix becomes available. 
    # pip-audit --ignore-vuln PYSEC-2022-43012
    safety check --key=${safetyAPIkey} -i 52495
  )
done

echo "All Good!"
