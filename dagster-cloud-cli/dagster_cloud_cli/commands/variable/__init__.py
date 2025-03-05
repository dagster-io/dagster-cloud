import json
import os
from typing import Dict, List, Optional, Any, Sequence

import typer
from typer import Typer

from ... import gql, ui
from ...config_utils import dagster_cloud_options

app = Typer(help="Commands for managing Dagster Cloud variables.")


def find_variable_by_name(variables: Sequence[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    """Find a variable by name, case-insensitive."""
    name_lower = name.lower()
    return next((s for s in variables if s["secretName"].lower() == name_lower), None)


def format_scopes_list(variable: Dict[str, Any]) -> List[str]:
    """Format the scopes of a variable into a list of strings."""
    scopes = []
    if variable["fullDeploymentScope"]:
        scopes.append("full")
    if variable["allBranchDeploymentsScope"]:
        scopes.append("branch-all")
    if variable["specificBranchDeploymentScope"]:
        scopes.append(f"branch-{variable['specificBranchDeploymentScope']}")
    if variable["localDeploymentScope"]:
        scopes.append("local")
    return scopes


def format_scopes_display(variable: Dict[str, Any]) -> List[str]:
    """Format the scopes of a variable into a list of display strings."""
    scopes = []
    if variable["fullDeploymentScope"]:
        scopes.append("Full deployment")
    if variable["allBranchDeploymentsScope"]:
        scopes.append("All branch deployments")
    if variable["specificBranchDeploymentScope"]:
        scopes.append(f"Specific branch: {variable['specificBranchDeploymentScope']}")
    if variable["localDeploymentScope"]:
        scopes.append("Local deployment")
    return scopes


def validate_scopes(
    full_deployment: bool,
    all_branch_deployments: bool,
    specific_branch_deployment: Optional[str],
    local_deployment: bool,
) -> None:
    """Validate that at least one scope is selected."""
    has_scope = (
        full_deployment
        or all_branch_deployments
        or specific_branch_deployment is not None
        or local_deployment
    )
    if not has_scope:
        ui.error("At least one scope must be selected")
        raise typer.Exit(1)


@app.command(name="list")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def list_variables(
    api_token: str,
    url: str,
    deployment: Optional[str],
    output_format: str = typer.Option(
        "table", "--output", help="Output format: table or json"
    ),
    full_deployment: Optional[bool] = typer.Option(
        None, "--full-deployment", help="Filter by full deployment scope"
    ),
    all_branch_deployments: Optional[bool] = typer.Option(
        None, "--all-branch-deployments", help="Filter by all branch deployments scope"
    ),
    specific_branch_deployment: Optional[str] = typer.Option(
        None, "--branch", help="Filter by specific branch deployment scope"
    ),
    local_deployment: Optional[bool] = typer.Option(
        None, "--local-deployment", help="Filter by local development scope"
    ),
    location: Optional[List[str]] = typer.Option(
        None, "--location", "-l", help="Filter by code location name. Can be specified multiple times."
    ),
):
    """List all variables in your deployment."""
    with gql.graphql_client_from_url(url, api_token, deployment_name=deployment) as client:
        try:
            variables = gql.get_all_secrets(client)
            
            # Filter variables based on scope criteria if provided
            if full_deployment is not None:
                variables = [s for s in variables if s["fullDeploymentScope"] == full_deployment]
                
            if all_branch_deployments is not None:
                variables = [s for s in variables if s["allBranchDeploymentsScope"] == all_branch_deployments]
                
            if specific_branch_deployment is not None:
                variables = [s for s in variables if s["specificBranchDeploymentScope"] == specific_branch_deployment]
                
            if local_deployment is not None:
                variables = [s for s in variables if s["localDeploymentScope"] == local_deployment]
                
            # Filter by location if specified
            if location:
                filtered_variables = []
                for s in variables:
                    # If variable has no locations, it applies to all locations
                    if not s["locationNames"] or any(loc in s["locationNames"] for loc in location):
                        filtered_variables.append(s)
                variables = filtered_variables
            
            if output_format.lower() == "json":
                ui.print(json.dumps(variables, indent=2))
            else:
                # Table format
                if not variables:
                    ui.print("No variables found")
                    return
                    
                headers = ["ID", "NAME", "LOCATIONS", "SCOPES", "UPDATED"]
                rows = []
                
                for variable in variables:
                    # Format scopes
                    scopes = format_scopes_list(variable)
                    
                    rows.append([
                        variable["id"],
                        variable["secretName"],
                        ", ".join(variable["locationNames"]) if variable["locationNames"] else "all",
                        ", ".join(scopes),
                        ui.format_timestamp(variable["updateTimestamp"]),
                    ])
                
                ui.print_table(headers, rows)
        except Exception as e:
            ui.error(f"Failed to list variables: {str(e)}")
            raise typer.Exit(1)


@app.command(name="describe")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def describe_variable(
    api_token: str,
    url: str,
    deployment: Optional[str],
    id: str = typer.Argument(..., help="ID of the variable to describe (get this using list or get-id)"),
    show_value: bool = typer.Option(
        False, "--show-value", help="Display the variable value (if permitted)"
    ),
):
    """Get details about a specific variable."""
    with gql.graphql_client_from_url(url, api_token, deployment_name=deployment) as client:
        try:
            variables = gql.get_all_secrets(client)
            
            # Find the variable by ID
            variable = next((v for v in variables if v["id"] == id), None)
            if not variable:
                ui.error(f"No variable found with ID '{id}'")
                raise typer.Exit(1)
            
            # Format output
            ui.print(f"Name: {variable['secretName']}")
            ui.print(f"ID: {variable['id']}")
            
            if show_value and variable["canViewSecretValue"]:
                ui.print(f"Value: {variable['secretValue']}")
            elif show_value:
                ui.print("Value: <not authorized to view>")
            
            ui.print(f"Updated by: {variable['updatedBy']['email']}")
            ui.print(f"Updated at: {ui.format_timestamp(variable['updateTimestamp'])}")
            ui.print(f"Locations: {', '.join(variable['locationNames']) if variable['locationNames'] else 'all'}")
            
            # Format scopes
            scopes = format_scopes_display(variable)
            ui.print(f"Scopes: {', '.join(scopes)}")
        except Exception as e:
            ui.error(f"Failed to get variable details: {str(e)}")
            raise typer.Exit(1)


@app.command(name="get-id")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def get_variable_id(
    api_token: str,
    url: str,
    deployment: Optional[str],
    name: str = typer.Argument(..., help="Name of the variable to get ID for"),
    full_deployment: Optional[bool] = typer.Option(
        None, "--full-deployment", help="Filter by full deployment scope"
    ),
    all_branch_deployments: Optional[bool] = typer.Option(
        None, "--all-branch-deployments", help="Filter by all branch deployments scope"
    ),
    specific_branch_deployment: Optional[str] = typer.Option(
        None, "--branch", help="Filter by specific branch deployment scope"
    ),
    local_deployment: Optional[bool] = typer.Option(
        None, "--local-deployment", help="Filter by local development scope"
    ),
    location: Optional[List[str]] = typer.Option(
        None, "--location", "-l", help="Filter by code location name. Can be specified multiple times."
    ),
):
    """Get only the ID of a specific variable, useful for update and delete operations."""
    with gql.graphql_client_from_url(url, api_token, deployment_name=deployment) as client:
        try:
            variables = gql.get_all_secrets(client)
            
            # Filter variables to match scope filters if provided
            matching_variables = []
            for variable in variables:
                if variable["secretName"].lower() != name.lower():
                    continue
                    
                if full_deployment is not None and variable["fullDeploymentScope"] != full_deployment:
                    continue
                    
                if all_branch_deployments is not None and variable["allBranchDeploymentsScope"] != all_branch_deployments:
                    continue
                    
                if specific_branch_deployment is not None and variable["specificBranchDeploymentScope"] != specific_branch_deployment:
                    continue
                    
                if local_deployment is not None and variable["localDeploymentScope"] != local_deployment:
                    continue
                
                # Filter by location if specified
                if location:
                    # If variable has no locations, it applies to all locations
                    if variable["locationNames"] and not any(loc in variable["locationNames"] for loc in location):
                        continue
                    
                matching_variables.append(variable)
            
            if not matching_variables:
                filters = []
                if full_deployment is not None:
                    filters.append(f"full-deployment={full_deployment}")
                if all_branch_deployments is not None:
                    filters.append(f"all-branch-deployments={all_branch_deployments}")
                if specific_branch_deployment is not None:
                    filters.append(f"branch={specific_branch_deployment}")
                if local_deployment is not None:
                    filters.append(f"local-deployment={local_deployment}")
                if location:
                    filters.append(f"location={', '.join(location)}")
                    
                filter_info = f" with the specified filters ({', '.join(filters)})" if filters else ""
                ui.error(f"Variable '{name}' not found{filter_info}")
                raise typer.Exit(1)
                
            # If multiple variables match our filters, notify the user and exit
            if len(matching_variables) > 1:
                ui.error(f"Multiple variables named '{name}' found with the specified filters. Please add more filters to uniquely identify the variable.")
                
                ui.print("\nFound the following matching variables:")
                for i, variable in enumerate(matching_variables, 1):
                    # Display scope and location info
                    scopes = format_scopes_display(variable)
                    locations = ", ".join(variable["locationNames"]) if variable["locationNames"] else "all locations"
                    ui.print(f"{i}. ID: {variable['id']} | Scopes: {', '.join(scopes)} | Locations: {locations}")
                
                ui.print("\nTo narrow results, try adding one or more of these filters:")
                ui.print("  --full-deployment=true/false")
                ui.print("  --all-branch-deployments=true/false")
                ui.print("  --branch=<branch-name>")
                ui.print("  --local-deployment=true/false")
                ui.print("  --location=<location-name>")
                raise typer.Exit(1)
            
            variable = matching_variables[0]
            
            # Output just the ID without any formatting or prefix
            # This makes it suitable for use in scripts
            ui.print(variable["id"], nl=False)
        except Exception as e:
            ui.error(f"Failed to get variable ID: {str(e)}")
            raise typer.Exit(1)


@app.command(name="get-value")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def get_variable_value(
    api_token: str,
    url: str,
    deployment: Optional[str],
    id: str = typer.Argument(..., help="ID of the variable to get value for (get this using list or get-id)"),
):
    """Get only the value of a specific variable, useful for scripting."""
    with gql.graphql_client_from_url(url, api_token, deployment_name=deployment) as client:
        try:
            variables = gql.get_all_secrets(client)
            
            # Find the variable by ID
            variable = next((v for v in variables if v["id"] == id), None)
            if not variable:
                ui.error(f"No variable found with ID '{id}'")
                raise typer.Exit(1)
            
            # Check if user can view the variable value
            if not variable["canViewSecretValue"]:
                ui.error("You don't have permission to view this variable's value")
                raise typer.Exit(1)
            
            # Output just the value without any formatting or prefix
            # This makes it suitable for use in scripts with command substitution
            ui.print(variable["secretValue"], nl=False)
        except Exception as e:
            ui.error(f"Failed to get variable value: {str(e)}")
            raise typer.Exit(1)


@app.command(name="create")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def create_variable(
    api_token: str,
    url: str,
    deployment: Optional[str],
    name: str = typer.Argument(..., help="Name for the new variable"),
    value: Optional[str] = typer.Option(
        None, "--value", "-v", help="Value for the variable"
    ),
    value_from_env: Optional[str] = typer.Option(
        None, "--from-env", help="Use value from the specified environment variable"
    ),
    location: Optional[List[str]] = typer.Option(
        None, "--location", "-l", help="Code location name(s) to scope the variable to. Can be specified multiple times."
    ),
    full_deployment: bool = typer.Option(
        True, "--full-deployment/--no-full-deployment", help="Apply to full deployment"
    ),
    all_branch_deployments: bool = typer.Option(
        False, "--all-branch-deployments", help="Apply to all branch deployments"
    ),
    specific_branch_deployment: Optional[str] = typer.Option(
        None, "--branch", help="Apply to a specific branch deployment"
    ),
    local_deployment: bool = typer.Option(
        False, "--local-deployment", help="Apply to local development"
    ),
):
    """Create a new variable."""
    # Validate input
    if value is not None and value_from_env is not None:
        ui.error("Cannot specify both --value and --from-env")
        raise typer.Exit(1)
    
    if value is None and value_from_env is None:
        ui.error("Must specify either --value or --from-env")
        raise typer.Exit(1)
    
    # Get value from environment if specified
    if value_from_env:
        value = os.environ.get(value_from_env)
        if not value:
            ui.error(f"Environment variable '{value_from_env}' not found or empty")
            raise typer.Exit(1)
    
    # Validate scopes
    validate_scopes(full_deployment, all_branch_deployments, specific_branch_deployment, local_deployment)
    
    scopes = {
        "fullDeploymentScope": full_deployment,
        "allBranchDeploymentsScope": all_branch_deployments,
        "specificBranchDeploymentScope": specific_branch_deployment,
        "localDeploymentScope": local_deployment,
    }
    
    # Default to all locations if none specified
    locations = location or []
    
    with gql.graphql_client_from_url(url, api_token, deployment_name=deployment) as client:
        try:
            # Check if variable already exists
            existing_variables = gql.get_all_secrets(client)
            existing_variable = find_variable_by_name(existing_variables, name)
            
            if existing_variable:
                ui.error(f"Variable '{name}' already exists. Use 'update' command to modify it.")
                raise typer.Exit(1)
            
            # Create the variable
            result = gql.create_secret(client, name, value, scopes, locations)
            ui.success(f"Variable '{name}' created successfully")
        except Exception as e:
            ui.error(f"Failed to create variable: {str(e)}")
            raise typer.Exit(1)


@app.command(name="update")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def update_variable(
    api_token: str,
    url: str,
    deployment: Optional[str],
    id: str = typer.Argument(..., help="ID of the variable to update (get this using get-id)"),
    value: Optional[str] = typer.Option(
        None, "--value", "-v", help="New value for the variable"
    ),
    value_from_env: Optional[str] = typer.Option(
        None, "--from-env", help="Use value from the specified environment variable"
    ),
    location: Optional[List[str]] = typer.Option(
        None, "--location", "-l", help="Code location name(s) to scope the variable to. Can be specified multiple times."
    ),
    # Options for new scope settings
    full_deployment: Optional[bool] = typer.Option(
        None, "--full-deployment/--no-full-deployment", help="Apply to full deployment"
    ),
    all_branch_deployments: Optional[bool] = typer.Option(
        None, "--all-branch-deployments/--no-all-branch-deployments", help="Apply to all branch deployments"
    ),
    specific_branch_deployment: Optional[str] = typer.Option(
        None, "--branch", help="Apply to a specific branch deployment"
    ),
    local_deployment: Optional[bool] = typer.Option(
        None, "--local-deployment/--no-local-deployment", help="Apply to local development"
    ),
):
    """Update an existing variable."""
    # Validate input
    if value is not None and value_from_env is not None:
        ui.error("Cannot specify both --value and --from-env")
        raise typer.Exit(1)
    
    # Get value from environment if specified
    if value_from_env:
        value = os.environ.get(value_from_env)
        if not value:
            ui.error(f"Environment variable '{value_from_env}' not found or empty")
            raise typer.Exit(1)
    
    with gql.graphql_client_from_url(url, api_token, deployment_name=deployment) as client:
        try:
            # Get all variables
            variables = gql.get_all_secrets(client)
            
            # Find the variable by ID
            variable = next((v for v in variables if v["id"] == id), None)
            if not variable:
                ui.error(f"No variable found with ID '{id}'")
                raise typer.Exit(1)
            
            if not variable["canEditSecret"]:
                ui.error(f"You don't have permission to edit variable '{variable['secretName']}'")
                raise typer.Exit(1)
            
            # Use existing values for any parameters not provided
            new_scopes = {
                "fullDeploymentScope": full_deployment if full_deployment is not None else variable["fullDeploymentScope"],
                "allBranchDeploymentsScope": all_branch_deployments if all_branch_deployments is not None else variable["allBranchDeploymentsScope"],
                "specificBranchDeploymentScope": specific_branch_deployment if specific_branch_deployment is not None else variable["specificBranchDeploymentScope"],
                "localDeploymentScope": local_deployment if local_deployment is not None else variable["localDeploymentScope"],
            }
            
            # Validate that at least one scope will be active
            validate_scopes(
                new_scopes["fullDeploymentScope"],
                new_scopes["allBranchDeploymentsScope"],
                new_scopes["specificBranchDeploymentScope"],
                new_scopes["localDeploymentScope"],
            )
            
            # Use existing locations if none provided
            locations = location if location is not None else variable["locationNames"]
            
            # Use existing value if none provided
            variable_value = value if value is not None else variable["secretValue"]
            
            # Show what's being updated
            scopes_before = format_scopes_display(variable)
            ui.print(f"Updating variable '{name}'")
            ui.print(f"Current scopes: {', '.join(scopes_before)}")
            
            # Create dummy variable dict with new scopes for display
            new_variable_for_display = {
                "fullDeploymentScope": new_scopes["fullDeploymentScope"],
                "allBranchDeploymentsScope": new_scopes["allBranchDeploymentsScope"],
                "specificBranchDeploymentScope": new_scopes["specificBranchDeploymentScope"],
                "localDeploymentScope": new_scopes["localDeploymentScope"],
            }
            scopes_after = format_scopes_display(new_variable_for_display)
            ui.print(f"New scopes: {', '.join(scopes_after)}")
            
            # Update the variable
            result = gql.update_secret(client, variable["id"], name, variable_value, new_scopes, locations)
            ui.success(f"Variable '{name}' updated successfully")
        except Exception as e:
            ui.error(f"Failed to update variable: {str(e)}")
            raise typer.Exit(1)


@app.command(name="delete")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def delete_variable(
    api_token: str,
    url: str,
    deployment: Optional[str],
    id: str = typer.Argument(..., help="ID of the variable to delete (get this using get-id)"),
    force: bool = typer.Option(
        False, "--force", "-f", help="Skip confirmation prompt"
    ),
):
    """Delete a variable."""
    with gql.graphql_client_from_url(url, api_token, deployment_name=deployment) as client:
        try:
            # Get all variables
            variables = gql.get_all_secrets(client)
            
            # Find the variable by ID
            variable = next((v for v in variables if v["id"] == id), None)
            if not variable:
                ui.error(f"No variable found with ID '{id}'")
                raise typer.Exit(1)
            
            # Confirm deletion if not forced
            if not force:
                confirmed = typer.confirm(f"Are you sure you want to delete variable '{variable['secretName']}'?")
                if not confirmed:
                    ui.print("Operation cancelled")
                    return
            
            # Delete the variable
            gql.delete_secret(client, variable["id"])
            ui.success(f"Variable '{variable['secretName']}' deleted successfully")
                
        except Exception as e:
            ui.error(f"Failed to delete variable: {str(e)}")
            raise typer.Exit(1)