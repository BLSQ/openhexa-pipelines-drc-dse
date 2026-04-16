import re
from datetime import datetime
from pathlib import Path

import papermill as pm
from openhexa.sdk import File, current_run, parameter, pipeline, workspace
from papermill import PapermillExecutionError

"""
Actualisation des données DSE

Ce pipeline OpenHexa met à jour les tables de la base de données à partir de deux fichiers: 
un fichier CSV pour les données de base de la DSE (Direction de la Surveillance Épidémiologique) et 
un fichier XLSX pour la complétude. 

Le pipeline suit ces étapes:

-Validation des entrées: vérification du format et de l'existence des fichiers.
-Exécution automatisée de notebooks Jupyter via Papermill, avec passage des chemins des fichiers en paramètres.
-La logique de mise à jour des tables de la base de données est intégrée dans ce pipeline, qui orchestre et 
  déclenche le processus à partir des données importées chaque semaine
"""

# Ticket:
# -https://bluesquare.atlassian.net/browse/PATHEOC-373
# Github:
# -https://github.com/BLSQ/openhexa-pipelines-drc-dse


@pipeline("dse_update")
@parameter(
    "dse_file",
    name="Base de donnees DSE (.csv)",
    type=File,
    required=True,
    default=None,
    help="Selectionnez un fichier .csv dans le dossier: 'tdb-suivi-epidemio/data/raw/DSE_data_YYYY/'",
)
@parameter(
    "completude_file",
    name="Fichier de completude (.xlsx)",
    type=File,
    required=True,
    default=None,
    help="Selectionnez un fichier .xlsx dans le dossier: 'tdb-suivi-epidemio/data/raw/Completude/YYYY/'",
)
def dse_update(dse_file: File, completude_file: File):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    current_run.log_info("Demarrage de la mise a jour de la base de donnees DSE...")
    pipeline_path = Path(workspace.files_path) / "tdb-suivi-epidemio"
    notebook_path = pipeline_path / "DSE_update_notebooks"
    superset_nb_path = pipeline_path / "superset_scripts"
    output_dir = pipeline_path / "papermill-outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check the file name year reference match that of the folder (hardcoded data paths require this consistency):
    match = re.match(r"(\d{4})_", dse_file.name)
    if match:
        year = match.group(1)

    # Build input paths:
    dse_input_file = pipeline_path / "data" / "raw" / f"DSE_data_{year}" / dse_file.name
    completude_input_file = pipeline_path / "data" / "raw" / "Completude" / f"{year}" / completude_file.name

    # ---------- input files validation ---------- #
    validate_inputs(
        notebook_path=notebook_path,
        dse_file=dse_input_file,
        completude_file=completude_input_file,
        output_dir=output_dir,
    )

    # ---------- data processing ---------- #
    run_notebook(
        nb_path=notebook_path / "01_process_mapepi_dse.ipynb",
        out_nb_path=output_dir,
        parameters={"nouvelles_donnees_path": dse_input_file.as_posix()},
    )

    run_notebook(
        nb_path=notebook_path / "02_malaria_data_for_pnlp_tdb.ipynb",
        out_nb_path=output_dir,
        parameters={"nouvelles_donnees_path": dse_input_file.as_posix()},
    )

    run_notebook(
        nb_path=notebook_path / "03_completude.ipynb",
        out_nb_path=output_dir,
        parameters={"nouvelle_completude_path": completude_input_file.as_posix()},
    )

    run_notebook(
        nb_path=notebook_path / "04_PNLP_dataset_update.ipynb",
        out_nb_path=output_dir,
        parameters={"nouvelles_donnees_path": dse_input_file.as_posix()},
        kernel_name="python3",
    )

    run_notebook(
        nb_path=notebook_path / "05_COUSP_dataset_update.ipynb",
        out_nb_path=output_dir,
        parameters={"nouvelles_donnees_path": dse_input_file.as_posix()},
        kernel_name="python3",
    )

    # ---------- Superset updates ---------- #
    run_notebook(
        nb_path=superset_nb_path / "DSE_Surv_Epi_Complete_agg_update.ipynb",
        out_nb_path=output_dir,
        kernel_name="python3",
    )

    run_notebook(
        nb_path=superset_nb_path / "DSE_Surv_Epi_Complete_agg_ss_update.ipynb",
        out_nb_path=output_dir,
        kernel_name="python3",
    )


def validate_inputs(notebook_path: Path, dse_file: Path, completude_file: Path, output_dir: Path):
    """Validate the existence and file type of the DSE and complétude files.

    Parameters
    ----------
    notebook_path : Path
        Path to the notebooks directory, used for logging and output management.
    dse_file : Path
        Path to the DSE .csv file.
    completude_file : Path
        Path to the complétude .xlsx file.
    output_dir : Path
        Directory where the output notebook will be saved.

    Raises
    ------
    ValueError
        If either file does not exist or has an incorrect extension.
    """
    if not dse_file.exists():
        current_run.log_error(
            f"Impossible de trouver le fichier dans le dossier correspondant {dse_file.parent}. "
            "Vérifiez que le dossier existe et contient bien le fichier DSE. "
            f"Chemin fourni: {dse_file}"
        )
        raise ValueError
    if dse_file.suffix.lower() != ".csv":
        current_run.log_error(f"Le fichier DSE doit être un fichier .csv valide. fichier fourni: {dse_file.name}")
        raise ValueError
    if not completude_file.exists():
        current_run.log_error(
            f"Impossible de trouver le fichier dans le dossier correspondant {completude_file.parent}. "
            "Vérifiez que le dossier existe et contient bien le fichier DSE. "
            f" Chemin fourni: {completude_file}"
        )
        raise ValueError
    if completude_file.suffix.lower() != ".xlsx":
        current_run.log_error(f"Le fichier de complétude doit être un fichier .xlsx valide. fichier fourni: {completude_file.name}")
        raise ValueError

    notebooks_to_run = [
        {
            "notebook": notebook_path / "00_validate_mapepi_csv.ipynb",
            "params": {"nouvelles_donnees_path": dse_file.as_posix()},
            "file_name": dse_file.name,
            "kernel_name": "python3",
        },
        {
            "notebook": notebook_path / "00_validate_completude.ipynb",
            "params": {"nouvelles_completude_path": completude_file.as_posix()},
            "file_name": completude_file.name,
            "kernel_name": "ir",
        },
    ]

    for nb in notebooks_to_run:
        try:
            current_run.log_debug(f"Validation du fichier: {nb['file_name']} via le notebook: {nb['notebook'].name}")
            run_notebook(
                nb_path=nb["notebook"],
                out_nb_path=output_dir,
                parameters=nb["params"],
                kernel_name=nb["kernel_name"],
            )
        except PapermillExecutionError as e:
            msg = e.evalue if hasattr(e, "evalue") else str(e)
            current_run.log_error(f"Échec de la validation du fichier '{nb['file_name']}': {msg} ")
            raise
        except Exception as e:
            current_run.log_error(f"Erreur lors de l'exécution du notebook: {nb['notebook'].name}: {e}")
            raise


def run_notebook(
    nb_path: Path,
    out_nb_path: Path,
    parameters: dict | None = None,
    kernel_name: str = "ir",
):
    """Execute a Jupyter notebook using Papermill.

    Notebook selection:
    - If country_code is provided, looks for a notebook named {stem}_{country_code}{suffix}
      in the same folder as nb_path (e.g. pipeline_NER.ipynb for country_code="NER").
    - If that file exists, it is executed; otherwise the default nb_path is executed.

    Parameters
    ----------
    nb_path : Path
        Path to the default notebook to execute.
    out_nb_path : Path
        Directory where the output notebook will be saved.
    parameters : dict
        Parameters passed to the notebook.
    kernel_name : str, optional
        Jupyter kernel name ("python3" for Python and "ir" for R).
    """
    current_run.log_info(f"Exécution du notebook: {nb_path.name}")
    file_stem = nb_path.stem
    extension = nb_path.suffix
    execution_timestamp = datetime.now().strftime("%Y%m%d")
    out_nb_full_path = out_nb_path / f"{file_stem}_OUTPUT_{execution_timestamp}{extension}"

    if parameters:
        current_run.log_debug(f"Parameters : {parameters}")

    try:
        pm.execute_notebook(
            input_path=nb_path,
            output_path=out_nb_full_path,
            parameters=parameters,
            kernel_name=kernel_name,
            request_save_on_cell_execute=False,
            progress_bar=False,
        )
    except PapermillExecutionError:
        current_run.log_error(f"Veuillez vérifier le notebook de sortie pour détecter d'eventuelles erreurs: {out_nb_full_path.name}")
        raise
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'exécution du notebook: {nb_path.name}: {e}")
        raise


if __name__ == "__main__":
    dse_update()
