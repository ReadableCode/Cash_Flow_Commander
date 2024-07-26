# Cash_Flow_Commander

## Setting Up

### Using Pipenv

#### Windows Pipenv and Pyenv setup

- Use chocolotey to install pyenv-win

```bash
choco install pyenv-win
```

- Install python 3.12

```bash
pyenv install 3.12.4
```

- Switch to this version of python globally

```bash
pyenv global 3.12.4
```

- Switch to this version of python locally
  
```bash
pyenv local 3.12.4
```

- Activate shell with pyenv
  
```bash
pyenv shell 3.12.4
```

- Install dependencies into this version of python

```bash
python3 -m pip install -r requirements.txt
```

- Using pipenv instead of installing to the local python version

```bash
pipenv install
```

- If this doesnt grab the right version of python from pyenv:
  
  ```bash
  pyenv which python
  pipenv --python <output of above command>
  # example
  pipenv --python C:\Users\jason\.pyenv\pyenv-win\versions\3.12.4\python.exe 
  ```

- Activate the pipenv in your powershell

  ```bash
  & "$(pipenv --venv)\Scripts\activate.ps1"
  ```
