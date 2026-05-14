import os
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
        commands = commands + [os.getenv("HIVE_BIN", "hive")]

    return [
        command
        for command in commands
        if shutil.which(command) is None
    ]


def missing_commands_for_all_pipelines():
    hive_command = os.getenv("HIVE_BIN", "hive")
    return sorted(
        {
            command
            for commands in list(PIPELINE_COMMANDS.values()) + [[hive_command]]
            for command in commands
            if shutil.which(command) is None
        }
    )


def all_environment_issues():
    return missing_env_for_groups(ENV_GROUPS.keys()), missing_commands_for_all_pipelines()


def pipeline_environment_issues(pipeline_name):
    return missing_env_for_pipeline(pipeline_name), missing_commands_for_pipeline(pipeline_name)


def has_environment_issues(missing_by_group, missing_commands):
    return bool(missing_by_group or missing_commands)


def print_environment_issues(missing_by_group, missing_commands, warning=False):
    label = "WARNING: " if warning else ""
    if missing_by_group:
        print(f"[-] {label}The following environment variables are missing:")
        for group_name, variables in missing_by_group.items():
            print(f"    {group_name}: {', '.join(variables)}")

    if missing_commands:
        print(f"\n[-] {label}The following commands are not available on PATH:")
        for command in missing_commands:
            print(f"    - {command}")

    print("\n[!] Please ensure PostgreSQL, Pig, Hadoop, Hive, and MongoDB are configured as needed.")
    print("[!] Source setup.sh or refer to README.md for setup instructions.")


def validate_runtime_environment(pipeline_name):
    """Fail early when the selected pipeline cannot find its configured runtime."""
    missing_by_group, missing_commands = pipeline_environment_issues(pipeline_name)

    if not has_environment_issues(missing_by_group, missing_commands):
        return

    print("[-] Runtime environment is not fully configured.")
    print_environment_issues(missing_by_group, missing_commands)
    raise EnvironmentError(f"{pipeline_name} runtime environment is incomplete")
