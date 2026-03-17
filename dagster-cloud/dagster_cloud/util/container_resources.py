import re

# Number-only regex.
NUMBERS_ONLY_REGEX = re.compile(r"^\d+$")
# Fractional decimal-allowed number regex.
FRACTIONAL_REGEX = re.compile(r"^\d*\.?\d+$")
# ACA memory uses K8s-style binary suffixes but allows fractional values (e.g. "0.5Gi", "1.5Gi").
ACA_MEM_QUANTITY_REGEX = re.compile(r"^(\d*\.?\d+)([EPTGMK])i$")
# K8s memory is in bytes when a plain number is provided, and in quantity strings when a string is provided.
K8S_MEM_QUANTITY_REGEX = re.compile(r"^(\d+)([EPTGMK])(i)?$")
# K8s CPU is in vCPUs when a number is provided, and in CPU units when a string ending in "m" is provided.
K8S_CPU_QUANTITY_REGEX = re.compile(r"^(\d+)(m?)$")
# ECS memory is MB when in integer, and GB when in string. Strings can contain spaces.
ECS_MEM_GB_REGEX = re.compile(r"^(\d*\.?\d+)(\s+)?GB$")
# ECS CPU is in CPU units when a number, and in vCPU when a string. Strings can contain spaces, and vcpu can be either lower or uppercase. We want to capture whether it's provided as a number or in terms of vCPUs.
ECS_CPU_VCPU_REGEX = re.compile(r"^(\d*\.?\d+)(\s+)?(v)?CPU$", flags=re.IGNORECASE)


def interpret_k8s_mem_str_as_bytes(mem_str: str | None) -> int | None:
    """Interpret a k8s memory quantity string as bytes."""
    # If the string is not provided, return None.
    if mem_str is None:
        return None

    # If the string is a number, then we interpret it as number of bytes.
    if NUMBERS_ONLY_REGEX.match(mem_str):
        return int(mem_str)

    # If the string is a quantity string, then we need to figure out the unit from the regex.
    match = K8S_MEM_QUANTITY_REGEX.match(mem_str)
    if match is None:
        raise Exception(f"Invalid k8s memory quantity string {mem_str}")

    # The first group is the number, the second group is the unit.
    num = int(match.group(1))
    unit = match.group(2)
    is_international_system = match.group(3) is not None

    if unit == "E":
        return num * 10**18 if not is_international_system else num * 2**60
    elif unit == "P":
        return num * 10**15 if not is_international_system else num * 2**50
    elif unit == "T":
        return num * 10**12 if not is_international_system else num * 2**40
    elif unit == "G":
        return num * 10**9 if not is_international_system else num * 2**30
    elif unit == "M":
        return num * 10**6 if not is_international_system else num * 2**20
    elif unit == "K":
        return num * 10**3 if not is_international_system else num * 2**10
    else:
        raise Exception(f"Invalid k8s memory quantity string {mem_str}")


def interpret_k8s_cpu_str_as_millicpus(cpu_str: str | None) -> int | None:
    """Interpret a k8s CPU quantity string as millicpus."""
    # If the string is not provided, return None.
    if cpu_str is None:
        return None

    # If the string is a number, then we interpret it as a fractional number of cpus.
    if FRACTIONAL_REGEX.match(cpu_str):
        return int(
            float(cpu_str) * 1000
        )  # K8s doesn't allow more precise than .001 CPU, meaning this will always be a whole number.

    # If the string is a quantity string, then we check to make sure it matches the millicpu syntax. Then we return the number.
    match = K8S_CPU_QUANTITY_REGEX.match(cpu_str)
    if match is None:
        raise Exception(f"Invalid k8s CPU quantity string {cpu_str}")

    # The first group is the number, the second group is the unit.
    return int(match.group(1))


def interpret_ecs_mem_str_as_bytes(mem_str: str | None) -> int | None:
    """Interpret an ECS memory string as bytes."""
    # If the string is not provided, return None.
    if mem_str is None:
        return None

    # If the string is a number, then we interpret it as number of MiB (mebibytes).
    if NUMBERS_ONLY_REGEX.match(mem_str):
        return int(mem_str) * 1024**2

    # If the string is a quantity string, then the unit should be GB.
    match = ECS_MEM_GB_REGEX.match(mem_str)
    if match is None:
        raise Exception(f"Invalid ECS memory string {mem_str}")

    # The first group is the number.
    num = int(match.group(1))
    return num * 1024**3


def interpret_aca_mem_str_as_bytes(mem_str: str | None) -> int | None:
    """Interpret an ACA memory string as bytes.

    ACA uses K8s-style binary suffixes but allows fractional values,
    e.g. "0.5Gi", "1.0Gi", "2Gi".
    """
    if mem_str is None:
        return None

    if NUMBERS_ONLY_REGEX.match(mem_str):
        return int(mem_str)

    match = ACA_MEM_QUANTITY_REGEX.match(mem_str)
    if match is None:
        raise Exception(f"Invalid ACA memory string {mem_str!r}. Expected format: '1.0Gi', '512Mi', etc.")

    num = float(match.group(1))
    unit = match.group(2)

    binary_units = {"E": 2**60, "P": 2**50, "T": 2**40, "G": 2**30, "M": 2**20, "K": 2**10}
    if unit not in binary_units:
        raise Exception(f"Invalid ACA memory string {mem_str!r}")

    return int(num * binary_units[unit])


def interpret_aca_cpu_str_as_millicpus(cpu_str: str | None) -> int | None:
    """Interpret an ACA CPU string as millicpus.

    ACA uses fractional vCPUs, e.g. 0.25, 0.5, 1.0, 2.0.
    """
    if cpu_str is None:
        return None

    if FRACTIONAL_REGEX.match(cpu_str):
        return int(float(cpu_str) * 1000)

    raise Exception(f"Invalid ACA CPU string {cpu_str!r}. Expected fractional vCPU value, e.g. '0.5', '1.0'.")


def interpret_ecs_cpu_str_as_millicpus(cpu_str: str | None) -> float | None:
    """Interpret an ECS CPU string as millicpus."""
    # If the string is not provided, return None.
    if cpu_str is None:
        return None

    # If the string is a number, then we interpret it as a whole number quantity of milliCPUs.
    if NUMBERS_ONLY_REGEX.match(cpu_str):
        return int(cpu_str)

    # If the string is a quantity string, then the unit should be vCPUs.
    match = ECS_CPU_VCPU_REGEX.match(cpu_str)
    if match is None:
        raise Exception(f"Invalid ECS CPU string {cpu_str}")

    # The first group is the number.
    num = float(match.group(1))
    return num * 1000
