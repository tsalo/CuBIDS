"""Console script for bond."""
import argparse
import subprocess
from pathlib import Path
import os
import sys
import re
import logging
from bond import BOnD
from .validator import (build_validator_call,
                        run_validator, parse_validator_output)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('bond-cli')
GIT_CONFIG = os.path.join(os.path.expanduser("~"), '.gitconfig')


def bond_validate():
    parser = argparse.ArgumentParser(
        description="bond-validate: Wrapper around the official "
        "BIDS Validator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('bids_dir',
                        type=Path,
                        action='store',
                        help='the root of a BIDS dataset. It should contain '
                        'sub-X directories and dataset_description.json')
    parser.add_argument('output_prefix',
                        type=Path,
                        action='store',
                        help='file prefix to which tabulated validator output '
                        'is written.')
    parser.add_argument('--container',
                        action='store',
                        help='Docker image tag or Singularity image file.',
                        default=None)

    parser.add_argument('--ignore_nifti_headers',
                        action='store_true',
                        default=False,
                        help='Disregard NIfTI header content during'
                        ' validation',
                        required=False)
    opts = parser.parse_args()

    # Run directly from python using subprocess
    if opts.container is None:
        call = build_validator_call(str(opts.bids_dir),
                                    opts.ignore_nifti_headers)
        ret = run_validator(call)

        if ret.returncode != 0:
            logger.error("Errors returned from validator run, parsing now")

        # parse the string output
        parsed = parse_validator_output(ret.stdout.decode('UTF-8'))
        if parsed.shape[1] < 1:
            logger.info("No issues/warnings parsed, your dataset"
                        " is BIDS valid.")
            sys.exit(0)
        else:
            logger.info("BIDS issues/warnings found in the dataset")

            if opts.output_prefix:
                # normally, write dataframe to file in CLI
                logger.info("Writing issues out to file")
                parsed.to_csv(str(opts.output_prefix) +
                              "_validation.csv", index=False)
                sys.exit(0)
            else:
                # user may be in python session, return dataframe
                return parsed

    # Run it through a container
    container_type = _get_container_type(opts.container)
    bids_dir_link = str(opts.bids_dir.absolute()) + ":/bids:ro"
    output_dir_link = str(opts.output_prefix.parent.absolute()) + ":/csv:rw"
    linked_output_prefix = "/csv/" + opts.output_prefix.name
    if container_type == 'docker':
        cmd = ['docker', 'run', '--rm', '-v', bids_dir_link,
               '-v', GIT_CONFIG+":/root/.gitconfig",
               '-v', output_dir_link, '--entrypoint', 'bond-validate',
               opts.container, '/bids', linked_output_prefix]
        if opts.ignore_nifti_headers:
            cmd.append('--ignore_nifti_headers')
    elif container_type == 'singularity':
        cmd = ['singularity', 'exec', '--cleanenv',
               '-B', bids_dir_link,
               '-B', output_dir_link, opts.container, 'bond-validate',
               '/bids', linked_output_prefix]
        if opts.ignore_nifti_headers:
            cmd.append('--ignore_nifti_headers')


    print("RUNNING: " + ' '.join(cmd))
    proc = subprocess.run(cmd)
    sys.exit(proc.returncode)


def bond_group():
    parser = argparse.ArgumentParser(
        description="bond-group: find key and parameter groups in BIDS",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('bids_dir',
                        type=Path,
                        action='store',
                        help='the root of a BIDS dataset. It should contain '
                        'sub-X directories and dataset_description.json')
    parser.add_argument('output_prefix',
                        type=Path,
                        action='store',
                        help='file prefix to which a _summary.csv, _files.csv '
                        'and _group.csv are written.')
    parser.add_argument('--container',
                        action='store',
                        help='Docker image tag or Singularity image file.')
    parser.add_argument('--use-datalad',
                        action='store_true',
                        help='ensure that there are no untracked changes '
                        'before finding groups')
    opts = parser.parse_args()

    # Run directly from python using
    if opts.container is None:
        bod = BOnD(data_root=str(opts.bids_dir),
                   use_datalad=opts.use_datalad)
        if opts.use_datalad and not bod.is_datalad_clean():
            raise Exception("Untracked change in " + str(opts.bids_dir))
        bod.get_CSVs(str(opts.output_prefix))
        sys.exit(0)

    # Run it through a container
    container_type = _get_container_type(opts.container)
    bids_dir_link = str(opts.bids_dir.absolute()) + ":/bids"
    output_dir_link = str(opts.output_prefix.parent.absolute()) + ":/csv:rw"
    linked_output_prefix = "/csv/" + opts.output_prefix.name
    if container_type == 'docker':
        cmd = ['docker', 'run', '--rm', '-v', bids_dir_link,
               '-v', GIT_CONFIG+":/root/.gitconfig",
               '-v', output_dir_link, '--entrypoint', 'bond-group',
               opts.container, '/bids', linked_output_prefix]
    elif container_type == 'singularity':
        cmd = ['singularity', 'exec', '--cleanenv',
               '-B', bids_dir_link,
               '-B', output_dir_link, opts.container, 'bond-group',
               '/bids', linked_output_prefix]
    if opts.use_datalad:
        cmd.append("--use-datalad")
    print("RUNNING: " + ' '.join(cmd))
    proc = subprocess.run(cmd)
    sys.exit(proc.returncode)


def bond_apply():
    parser = argparse.ArgumentParser(
        description="bond-apply: apply the changes specified in a csv "
        "to a BIDS directory",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('bids_dir',
                        type=Path,
                        action='store',
                        help='the root of a BIDS dataset. It should contain '
                        'sub-X directories and dataset_description.json')
    parser.add_argument('edited_csv_prefix',
                        type=Path,
                        action='store',
                        help='file prefix used to store the _summary.csv, '
                        '_files.csv and _group.csv that have been edited.')
    parser.add_argument('new_csv_prefix',
                        type=Path,
                        action='store',
                        help='file prefix for writing the new _summary.csv, '
                        '_files.csv and _group.csv that have been edited.')
    parser.add_argument('--container',
                        action='store',
                        help='Docker image tag or Singularity image file.')
    parser.add_argument('--use-datalad',
                        action='store_true',
                        help='ensure that there are no untracked changes '
                        'before applying changes')
    opts = parser.parse_args()

    # Run directly from python using
    if opts.container is None:
        bod = BOnD(data_root=str(opts.bids_dir),
                   use_datalad=opts.use_datalad)
        if opts.use_datalad and not bod.is_datalad_clean():
            raise Exception("Untracked change in " + str(opts.bids_dir))
        bod.apply_csv_changes(str(opts.edited_csv_prefix),
                              str(opts.new_csv_prefix))
        sys.exit(0)

    # Run it through a container
    container_type = _get_container_type(opts.container)
    bids_dir_link = str(opts.bids_dir.absolute()) + ":/bids"
    input_csv_dir_link = str(opts.edited_csv_prefix.parent.absolute()) \
        + ":/in_csv:ro"
    output_csv_dir_link = str(opts.new_csv_prefix.parent.absolute()) \
        + ":/out_csv:rw"
    linked_input_prefix = "/out_csv/" + opts.edited_csv_prefix.name
    linked_output_prefix = "/out_csv/" + opts.new_csv_prefix.name
    if container_type == 'docker':
        cmd = ['docker', 'run', '--rm',
               '-v', bids_dir_link,
               '-v', GIT_CONFIG+":/root/.gitconfig",
               '-v', input_csv_dir_link,
               '-v', output_csv_dir_link,
               '--entrypoint', 'bond-apply',
               opts.container, '/bids', linked_input_prefix,
               linked_output_prefix]

    elif container_type == 'singularity':
        cmd = ['singularity', 'exec', '--cleanenv',
               '-B', bids_dir_link,
               '-B', input_csv_dir_link,
               '-B', output_csv_dir_link,
               opts.container, 'bond-apply',
               '/bids', linked_input_prefix,
               linked_output_prefix]
    if opts.use_datalad:
        cmd.append("--use-datalad")
    print("RUNNING: " + ' '.join(cmd))
    proc = subprocess.run(cmd)
    sys.exit(proc.returncode)


def param_group_merge():
    pass


def bond_datalad_save():
    parser = argparse.ArgumentParser(
        description="bond-datalad-save: perform a DataLad save on a BIDS "
        "directory",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('bids_dir',
                        type=Path,
                        action='store',
                        help='the root of a BIDS dataset. It should contain '
                        'sub-X directories and dataset_description.json')
    parser.add_argument('-m',
                        action='store',
                        help='message for this commit')
    parser.add_argument('--container',
                        action='store',
                        help='Docker image tag or Singularity image file.')
    opts = parser.parse_args()

    # Run directly from python using
    if opts.container is None:
        bod = BOnD(data_root=str(opts.bids_dir), use_datalad=True)
        bod.datalad_save(message=opts.m)
        sys.exit(0)

    # Run it through a container
    container_type = _get_container_type(opts.container)
    bids_dir_link = str(opts.bids_dir.absolute()) + ":/bids"
    if container_type == 'docker':
        cmd = ['docker', 'run', '--rm', '-v', bids_dir_link,
               '-v', GIT_CONFIG+":/root/.gitconfig",
               '--entrypoint', 'bond-datalad-save',
               opts.container, '/bids', '-m', opts.m]
    elif container_type == 'singularity':
        cmd = ['singularity', 'exec', '--cleanenv',
               '-B', bids_dir_link,
               opts.container, 'bond-datalad-save',
               '/bids', '-m', opts.m]
    print("RUNNING: " + ' '.join(cmd))
    proc = subprocess.run(cmd)
    sys.exit(proc.returncode)


def bond_undo():
    parser = argparse.ArgumentParser(
        description="bond-undo: revert most recent commit",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('bids_dir',
                        type=Path,
                        action='store',
                        help='the root of a BIDS dataset. It should contain '
                        'sub-X directories and dataset_description.json')
    parser.add_argument('--container',
                        action='store',
                        help='Docker image tag or Singularity image file.')
    opts = parser.parse_args()

    # Run directly from python using
    if opts.container is None:
        bod = BOnD(data_root=str(opts.bids_dir), use_datalad=True)
        bod.datalad_undo_last_commit()
        sys.exit(0)

    # Run it through a container
    container_type = _get_container_type(opts.container)
    bids_dir_link = str(opts.bids_dir.absolute()) + ":/bids"
    if container_type == 'docker':
        cmd = ['docker', 'run', '--rm', '-v', bids_dir_link,
               '-v', GIT_CONFIG+":/root/.gitconfig",
               '--entrypoint', 'bond-undo',
               opts.container, '/bids']
    elif container_type == 'singularity':
        cmd = ['singularity', 'exec', '--cleanenv',
               '-B', bids_dir_link,
               opts.container, 'bond-undo', '/bids']
    print("RUNNING: " + ' '.join(cmd))
    proc = subprocess.run(cmd)
    sys.exit(proc.returncode)


def bond_remove_metadata_fields():
    parser = argparse.ArgumentParser(
        description="bond-remove-metadata-fields: delete fields from "
        "metadata",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('bids_dir',
                        type=Path,
                        action='store',
                        help='the root of a BIDS dataset. It should contain '
                        'sub-X directories and dataset_description.json')
    parser.add_argument('--fields',
                        nargs='+',
                        action='store',
                        default=[],
                        help='space-separated list of metadata fields to '
                        'remove.')
    parser.add_argument('--container',
                        action='store',
                        help='Docker image tag or Singularity image file.')
    opts = parser.parse_args()

    # Run directly from python
    if opts.container is None:
        bod = BOnD(data_root=str(opts.bids_dir), use_datalad=False)
        bod.remove_metadata_fields(opts.fields)
        sys.exit(0)

    # Run it through a container
    container_type = _get_container_type(opts.container)
    bids_dir_link = str(opts.bids_dir.absolute()) + ":/bids:rw"
    if container_type == 'docker':
        cmd = ['docker', 'run', '--rm', '-v', bids_dir_link,
               '--entrypoint', 'bond-remove-metadata-fields',
               opts.container, '/bids', '--fields'] + opts.fields
    elif container_type == 'singularity':
        cmd = ['singularity', 'exec', '--cleanenv',
               '-B', bids_dir_link,
               opts.container, 'bond-remove-metadata-fields',
               '/bids', '--fields'] + opts.fields
    print("RUNNING: " + ' '.join(cmd))
    proc = subprocess.run(cmd)
    sys.exit(proc.returncode)


def bond_print_metadata_fields():
    parser = argparse.ArgumentParser(
        description="bond-print-metadata-fields: print all unique "
        "metadata fields",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('bids_dir',
                        type=Path,
                        action='store',
                        help='the root of a BIDS dataset. It should contain '
                        'sub-X directories and dataset_description.json')
    parser.add_argument('--container',
                        action='store',
                        help='Docker image tag or Singularity image file.')
    opts = parser.parse_args()

    # Run directly from python
    if opts.container is None:
        bod = BOnD(data_root=str(opts.bids_dir), use_datalad=False)
        fields = bod.get_all_metadata_fields()
        print("\n".join(fields))
        sys.exit(0)

    # Run it through a container
    container_type = _get_container_type(opts.container)
    bids_dir_link = str(opts.bids_dir.absolute()) + ":/bids:ro"
    if container_type == 'docker':
        cmd = ['docker', 'run', '--rm', '-v', bids_dir_link,
               '--entrypoint', 'bond-print-metadata-fields',
               opts.container, '/bids']
    elif container_type == 'singularity':
        cmd = ['singularity', 'exec', '--cleanenv',
               '-B', bids_dir_link,
               opts.container, 'bond-print-metadata-fields',
               '/bids']
    print("RUNNING: " + ' '.join(cmd))
    proc = subprocess.run(cmd)
    sys.exit(proc.returncode)


def _get_container_type(image_name):

    # If it's a file on disk, it must be a singularity image
    if Path(image_name).exists():
        return "singularity"

    # It needs to match a docker tag pattern to be docker
    if re.match(r"(?:.+\/)?([^:]+)(?::.+)?", image_name):
        return "docker"

    raise Exception("Unable to determine the container type of " + image_name)
