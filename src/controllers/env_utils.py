import os
import importlib.util
import shutil


ENV_GROUPS = {
    "PostgreSQL": ["PGDATABASE", "PGUSER", "PGPASSWORD", "PGHOST", "PGPORT"],
    "Java": ["JAVA_HOME"],
    "Pig": ["PIG_HOME", "PIG_CLASSPATH"],
    "Hadoop": ["HADOOP_HOME", "HADOOP_CONF_DIR"],
    "Hive": ["HIVE_HOME"],
    "MongoDB": ["MONGO_URI", "MONGO_DB"],
}

PIPELINE_ENV_GROUPS = {
    "pig": ["PostgreSQL", "Java", "Pig"],
    "mapreduce": ["PostgreSQL"],
    "hive": ["PostgreSQL", "Java", "Hadoop", "Hive"],
    "mongodb": ["PostgreSQL", "MongoDB"],
}

PIPELINE_COMMANDS = {
    "pig": ["pig"],
    "hive": ["hadoop"],
}

PIPELINE_PYTHON_MODULES = {
    "mongodb": ["pymongo"],
}

PIPELINE_DISPLAY_NAMES = {
    "pig": "Pig",
    "mapreduce": "MapReduce",
    "hive": "Hive",
    "mongodb": "MongoDB",
}


def missing_env_for_groups(group_names):
    missing_by_group = {}
    for group_name in group_names:
        missing = [var for var in ENV_GROUPS[group_name] if not os.getenv(var)]
        if missing:
            missing_by_group[group_name] = missing
    return missing_by_group


def missing_env_for_pipeline(pipeline_name):
    return missing_env_for_groups(PIPELINE_ENV_GROUPS[pipeline_name])


def missing_commands_for_pipeline(pipeline_name):
    commands = PIPELINE_COMMANDS.get(pipeline_name, [])
    if pipeline_name == "hive":
        hive_home = os.getenv("HIVE_HOME")
        beeline_command = os.getenv("HIVE_BEELINE_BIN")
        if not beeline_command and hive_home:
            beeline_command = os.path.join(hive_home, "bin", "beeline")

        commands = commands + [
            os.getenv("HIVE_BIN", "hive"),
            beeline_command or "beeline",
        ]

    return [
        command
        for command in commands
        if shutil.which(command) is None
    ]


def missing_python_modules_for_pipeline(pipeline_name):
    modules = PIPELINE_PYTHON_MODULES.get(pipeline_name, [])
    return [
        module_name
        for module_name in modules
        if importlib.util.find_spec(module_name) is None
    ]


def missing_commands_for_all_pipelines():
    hive_command = os.getenv("HIVE_BIN", "hive")
    hive_home = os.getenv("HIVE_HOME")
    beeline_command = os.getenv("HIVE_BEELINE_BIN")
    if not beeline_command and hive_home:
        beeline_command = os.path.join(hive_home, "bin", "beeline")

    return sorted(
        {
            command
            for commands in list(PIPELINE_COMMANDS.values()) + [[hive_command, beeline_command or "beeline"]]
            for command in commands
            if shutil.which(command) is None
        }
    )


def all_environment_issues():
    return missing_env_for_groups(ENV_GROUPS.keys()), missing_commands_for_all_pipelines()


def pipeline_environment_issues(pipeline_name):
    return (
        missing_env_for_pipeline(pipeline_name),
        missing_commands_for_pipeline(pipeline_name),
        missing_python_modules_for_pipeline(pipeline_name),
    )


def has_environment_issues(missing_by_group, missing_commands, missing_python_modules=None):
    return bool(missing_by_group or missing_commands or missing_python_modules)


def print_environment_issues(missing_by_group, missing_commands, missing_python_modules=None, warning=False):
    label = "WARNING: " if warning else ""
    if missing_by_group:
        print(f"[-] {label}The following environment variables are missing:")
        for group_name, variables in missing_by_group.items():
            print(f"    {group_name}: {', '.join(variables)}")

    if missing_commands:
        print(f"\n[-] {label}The following commands are not available on PATH:")
        for command in missing_commands:
            print(f"    - {command}")

    if missing_python_modules:
        print(f"\n[-] {label}The following Python packages are not installed in this interpreter:")
        for module_name in missing_python_modules:
            print(f"    - {module_name}")
        print("    Run: python -m pip install -r requirements.txt")

    print("\n[!] Please ensure PostgreSQL, Pig, Hadoop, Hive, and MongoDB are configured as needed.")
    print("[!] Source setup.sh or refer to README.md for setup instructions.")


def validate_runtime_environment(pipeline_name):
    """Fail early when the selected pipeline cannot find its configured runtime."""
    missing_by_group, missing_commands, missing_python_modules = pipeline_environment_issues(pipeline_name)

    if not has_environment_issues(missing_by_group, missing_commands, missing_python_modules):
        return

    print("[-] Runtime environment is not fully configured.")
    print_environment_issues(missing_by_group, missing_commands, missing_python_modules)
    raise EnvironmentError(f"{pipeline_name} runtime environment is incomplete")
