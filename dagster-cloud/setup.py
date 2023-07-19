from pathlib import Path
from typing import Dict

from setuptools import find_packages, setup


def get_version() -> str:
    version: Dict[str, str] = {}
    with open(
        Path(__file__).parent / "dagster_cloud/version.py", encoding="utf8"
    ) as fp:
        exec(fp.read(), version)

    return version["__version__"]


def get_description() -> str:
    return (Path(__file__).parent / "README.md").read_text()


ver = get_version()
# dont pin dev installs to avoid pip dep resolver issues
pin = "" if ver == "1!0+dev" else f"=={ver}"
setup(
    name="dagster_cloud",
    long_description=get_description(),
    long_description_content_type="text/markdown",
    version=ver,
    author_email="support@elementl.com",
    project_urls={
        "Homepage": "https://dagster.io/cloud",
        "GitHub": "https://github.com/dagster-io/dagster-cloud",
        "Changelog": "https://github.com/dagster-io/dagster-cloud/blob/main/CHANGES.md",
        "Issue Tracker": "https://github.com/dagster-io/dagster-cloud/issues",
        "Twitter": "https://twitter.com/dagster",
        "LinkedIn": "https://www.linkedin.com/showcase/dagster",
        "YouTube": "https://www.youtube.com/channel/UCfLnv9X8jyHTe6gJ4hVBo9Q",
        "Slack": "https://dagster.io/slack",
        "Blog": "https://dagster.io/blog",
        "Newsletter": "https://dagster.io/newsletter-signup",
    },
    packages=find_packages(exclude=["dagster_cloud_tests*"]),
    include_package_data=True,
    install_requires=[
        "dagster",
        "dagster-cloud-cli",
        "pex",
        "questionary",
        "requests",
        "typer[all]",
    ],
    extras_require={
        "tests": [
            "black",
            "docker",
            "httpretty",
            "isort",
            "kubernetes",
            "moto[all]",
            "mypy",
            "paramiko",
            "pylint",
            "pytest",
            "types-PyYAML",
            "types-requests",
            "dagster-cloud-test-infra",
            "dagster_k8s==0.19.14",
        ],
        "docker": ["docker", "dagster_docker==0.19.14"],
        "kubernetes": ["kubernetes", "dagster_k8s==0.19.14"],
        "ecs": ["dagster_aws==0.19.14", "boto3"],
        "sandbox": ["supervisor"],
        "pex": ["boto3"],
        "serverless": ["boto3"],
    },
    author="Elementl",
    license="Apache-2.0",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: System :: Monitoring",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Operating System :: OS Independent",
    ],
)
