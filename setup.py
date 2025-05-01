from setuptools import setup, find_packages

setup(
    name="gdrive-duplicate-scanner",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "google-api-python-client>=2.0.0",
        "google-auth-httplib2>=0.1.0",
        "google-auth-oauthlib>=0.4.0",
        "tqdm>=4.65.0",
    ],
    python_requires=">=3.10",
) 