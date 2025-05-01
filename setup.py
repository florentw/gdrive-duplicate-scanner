from setuptools import setup, find_packages

setup(
    name="g-drive-remove-duplicates",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "google-api-python-client",
        "google-auth-httplib2",
        "google-auth-oauthlib",
    ],
    python_requires=">=3.6",
) 