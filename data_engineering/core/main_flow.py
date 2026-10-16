import argparse
import os
import time
from datetime import datetime, timedelta

from data_engineering.core.utils import execute_flows, load_yaml_file, validate_folders


def main():
    """
    This script, `main_flow.py`, serves as the main execution flow for running specific
    data processes in a data engineering environment. Depending on the type of flow
    specified, it will execute different processes such as cataloging, data cleaning, or
    data transformation.

    Functions:
        main(): Main function that handles argument parsing, configuration loading,
        date range determination, strategy validation, and data flow execution.

    Usage:
        In a local environment, execute this script from the project root using:
        python -m data_engineering.core.main_flow --env <environment> --flow <flow>
        [--overwrite_strategy <strategy>] [--start_dt <start_date>]
        [--end_dt <end_date>] [--nodes <nodes>] [--sanity_check]

    Arguments:
        --env (str): The environment in which the flow will be executed. This argument
        is required.
        --flow (str): The main data flow to execute. This argument is required and
        determines which processes will run (e.g., catalog_flow, snapshot_flow,
        transactional_flow, data_transformation_flow).
        --overwrite_strategy (str, optional): Overwrite strategy to use. Can be
        "overwrite" or "overwriteSchema". Defaults to "overwrite".
        --start_dt (str, optional): Start date in 'YYYY-MM-DD' format. If not provided,
        defaults to the first day of the previous month.
        --end_dt (str, optional): End date in 'YYYY-MM-DD' format. If not provided,
        defaults to the last day of the previous month.
        --nodes (str, optional): Specific nodes to execute, separated by commas. If
        provided, only these nodes will be executed.
        --sanity_check (str, optional): Enable/disable sanity_check_flow execution.
        Accepts 'true' (default) or 'false'. Sanity check runs automatically after
        main flows complete, validating data quality. When used with --nodes, runs
        in isolated mode (validates only the specified domain).

    Default Behavior:
        If only the --env and --flow arguments are provided, the script will default to
        using the start date as the first day of the previous month and the end date as
        the last day of the previous month. The overwrite strategy will default to
        "overwrite".

    Exceptions:
        ValueError: Raised if an invalid overwrite strategy is provided or if specified
        nodes do not exist in the flow catalog.
        argparse.ArgumentError: Raised if both --start_dt and --end_dt arguments are not
        provided when required.

    Output:
        Informational messages about the flow execution, including total execution time
        or any unexpected errors that occur.

    Examples:
        # Example with specific start and end dates
        python -m data_engineering.core.main_flow --env sandbox/sandbox_name --flow \
        data_transformation_flow --start_dt 2023-01-01 --end_dt 2023-01-31

        # Example with specific nodes
        python -m data_engineering.core.main_flow --env sandbox/sandbox_name --flow \
        snapshot_flow --nodes customer.customer_flow,customer.income_flow

        # Example with overwrite strategy
        python -m data_engineering.core.main_flow --env sandbox/sandbox_name --flow \
        catalog_flow --overwrite_strategy overwriteSchema

        # Example with only environment and flow
        python -m data_engineering.core.main_flow \
        --env sandbox/sandbox_name --flow transactional_flow

    """
    try:
        start_time = time.time()
        os.environ["root_dir"] = root_dir = os.path.dirname(os.path.dirname(__file__))
        parser = argparse.ArgumentParser(description="run")
        parser.add_argument("--env", type=str, required=True)
        parser.add_argument("--workflow", type=str, required=True)
        parser.add_argument("--flow", type=str, required=True)
        parser.add_argument(
            "--overwrite_strategy", default="replaceWhere", type=str, required=False
        )
        parser.add_argument("--start_dt", default="", type=str, required=False)
        parser.add_argument("--end_dt", default="", type=str, required=False)
        parser.add_argument(
            "--nodes",
            type=lambda s: [element.strip() for element in s.split(",")],
            required=False,
        )
        parser.add_argument(
            "--sanity_check",
            type=str,
            default="false",
            choices=["true", "false"],
            required=False,
            help=(
                "Execute sanity_check_flow after main flows (default: false). "
                "Set to 'false' to disable."
            ),
        )

        args = parser.parse_args()

        if not args.start_dt and not args.end_dt:
            if args.flow not in ["raw_data_flow"]:
                today = datetime.today()
                first_day_last_month = (
                    today.replace(day=1) - timedelta(days=1)
                ).replace(day=1)
                last_day_last_month = today.replace(day=1) - timedelta(days=1)
                args.start_dt = first_day_last_month.strftime("%Y-%m-%d")
                args.end_dt = last_day_last_month.strftime("%Y-%m-%d")
                print(
                    f"ℹ️ [INFO] | Default parameters loaded for this "
                    f"execution: start_dt = {args.start_dt}, "
                    f"end_dt = {args.end_dt}"
                )
        elif not args.start_dt or not args.end_dt:
            parser.error("start_dt and end_dt are required for this flow")

        valid_strategies = ["replaceWhere", "overwriteSchema"]
        data_folder = args.workflow
        valid_folder = validate_folders(root_dir, data_folder)

        if args.overwrite_strategy not in valid_strategies:
            raise ValueError(
                f"Error: {args.overwrite_strategy} is not a valid strategy, "
                f"valid strategies are: {valid_strategies}"
            )
        elif not valid_folder:
            raise ValueError(f"Error: {data_folder} is not a valid workflow, ")

        data = load_yaml_file(os.path.join(root_dir, f"{data_folder}/conf/flow.yml"))

        flows = data.get(args.flow, [])

        if args.nodes and args.nodes[0].strip() != "":
            non_existing_nodes = set(args.nodes) - set(flows)
            if non_existing_nodes:
                raise ValueError(
                    f"Error: the next nodes don't exist in the "
                    f"flow catalog file: {non_existing_nodes}"
                )

            flows = list(set(flows).intersection(args.nodes))

        text_lookup = data_folder + "_folder"
        module = load_yaml_file(
            os.path.join(root_dir, "env/base/global_settings.yml")
        ).get(text_lookup, "")
        flows = [module + element for element in flows]

        os.environ["folder"] = data_folder
        os.environ["env"] = args.env
        os.environ["overwrite_strategy"] = args.overwrite_strategy
        os.environ["start_dt"] = args.start_dt
        os.environ["end_dt"] = args.end_dt
        os.environ["workflow"] = args.workflow

        sanity_check_default = args.sanity_check.lower() == "true"
        os.environ["auto_sanity_check_enabled"] = "1" if sanity_check_default else "0"
        os.environ["auto_sanity_check_layer"] = args.workflow

        execute_flows(
            flows,
            args.flow,
            p_sanity_check=sanity_check_default,
            p_nodes=args.nodes,
            workflow=args.workflow,
        )

        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, seconds = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(seconds, 60)
        print(
            f"✅ [SUCCESS] Total execution time: {int(hours)} "
            f"hours, {int(minutes)} minutes, {seconds:.2f} seconds "
        )

    except Exception as e:
        raise Exception(f"❌ [ERROR] An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
