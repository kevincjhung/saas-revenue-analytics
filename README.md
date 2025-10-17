# sales-pipeline

## Miniforge/Conda

✅ Step 1: Create a New Environment

```conda create -n salespipeline python=3.13 pandas```

-n finance: the name of the environment
python=3.13 pandas: installs Python 3.13 and pandas into it


✅ Step 2: Activate the Environment

```conda activate salespipeline```
Your terminal prompt will now look like this:

```(salespipeline) kevincjhung@Kevins-MacBook-Pro %```
This means you're "inside" the finance environment. Every package you install now is isolated to this environment.

✅ Step 3: Run your script
Make sure you're in your project folder, then run:

```python main.py```

It will now use the Python and pandas from your finance environment.

✅ Step 4: Add More Packages (Optional)

```conda install matplotlib```

Or for non-Conda packages:

```pip install openpyxl```

Yes, you can use pip inside a conda environment — just try to prefer conda if the package exists there.

Deactivating the Environment
```conda deactivate```

List all environments
```conda env list``` or ```conda info --envs```

Remove environmen
```conda remove -n finance --all```

Export environment file	
```conda env export > environment.yml```

Recreate from file	
```conda env create -f environment.yml```

## Testing

```python -m pytest```

## Database

Initialize database
```python -m salespipeline.db.init_db```
