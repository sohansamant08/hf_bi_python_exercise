# Instructions to run the code

* Assuming the OS is OSX/Linux and python version 3

* Download following packages to run the code if it doesn't exist.

```sh
pip3 install pytest
pip3 install requests
pip3 install isodate
pip3 install pandas
pip3 install numpy
pip3 install datetime
```

* Export python path using following command `export PYTHONPATH=../src:$PYTHONPATH` to set path correctly.

* Directory structure of the project -
  * Create Parent level folder: `hf_bi_python_exercise`
  * Actual source code folder and file: `hf_bi_python_exercise/recipes-etl/src/main.py`.

  * Output file folder: All output file is stored in `outputFile/` folder. Create that folder inside `hf_bi_python_exercise/recipes-etl/`. It will holder results csv, one is for `Chiles.csv` and `Results.csv`.
    * Chiles.csv - Is the actual result after performing the ETL.
    * Results.csv - Is the final result with 3 rows only where data is aggregated by average total time and grouped by difficulty level.

  * Config File: Create Config file in `hf_bi_python_exercise/recipes-etl/src/` folder and name it as `config.json`.
    * Config file is a json based file which has source file location and output file location.
    * This locations should be changed to match the location where this job will run.

* Using Python to run the ELT job:
* Go to the root of the directory using `cd hf_bi_python_exercise/recipes-etl/` and then run following command:

```sh
python src/main.py
```

* Unit Tests:
  * Create unit tests folder inside `hf_bi_python_exercise/recipes-etl/` and store Unit tests in `hf_bi_python_exercise/recipes-etl/tests/` folder.

* Command to run the unit test cases:
* Go to the parent folder using `cd hf_bi_python_exercise/recipes-etl` and then run following command:

```sh
pytest tests/unitTest.py 
```

* We can use shell script to run the job as well. The script would be simple to call just python file
