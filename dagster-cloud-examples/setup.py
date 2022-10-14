from typing import Dict

from setuptools import find_packages, setup


def get_version() -> str:
    version: Dict[str, str] = {}
    with open("dagster_cloud_examples/version.py", encoding="utf8") as fp:
        exec(fp.read(), version)  # pylint: disable=W0122

    return version["__version__"]


if __name__ == "__main__":
    ver = get_version()
    # dont pin dev installs to avoid pip dep resolver issues
    pin = "" if ver == "0+dev" else f"=={ver}"
    setup(
        name="dagster_cloud_examples",
        version=ver,
        packages=find_packages(exclude=["dagster_cloud_examples_tests*"]),
        install_requires=["dagster_cloud==1.0.13"],
        extras_require={"tests": ["mypy", "pylint", "pytest"]},
        author="Elementl",
        author_email="hello@elementl.com",
        license="Apache-2.0",
        classifiers=[
            "Programming Language :: Python :: 3.8",
            "License :: OSI Approved :: Apache Software License",
            "Operating System :: OS Independent",
        ],
    )
