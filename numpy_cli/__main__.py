import click
import numpy as np
import yaml
import sys

@click.command()
@click.argument('python_command', type=str)
def cli(python_command):
    # Read YAML data from stdin
    yaml_data = yaml.safe_load(sys.stdin)

    # Convert the YAML data to a NumPy array
    a = np.array(yaml_data)

    python_command += "; click.echo(yaml.dump(a.tolist()))"
    
    # Execute the provided Python command on the array 'a'
    exec(python_command, globals(), locals())

if __name__ == "__main__":
    cli()
