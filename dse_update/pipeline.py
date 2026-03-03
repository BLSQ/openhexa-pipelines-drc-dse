from datetime import datetime
from pathlib import Path

import papermill as pm
from openhexa.sdk import File, current_run, parameter, pipeline, workspace
from papermill import PapermillExecutionError

"""
Actualisation des données DSE

Ce pipeline OpenHexa permet de mettre à jour les tables de la base de données. Il utilise en entrée deux fichiers : un fichier CSV pour les données de base de la DSE (Direction de la Surveillance Épidémiologique) et un fichier XLSX pour les données de complétude.
Le pipeline suit ces étapes :
Validation des entrées : Il vérifie que le fichier DSE est bien un .csv et que le fichier de complétude est un .xlsx, et que les deux fichiers existent.
Exécution du Notebook Jupyter : Il orchestre l'exécution d'un notebook Jupyter spécifique (LAUNCHER.ipynb) via Papermill. Les chemins des fichiers d'entrée sont passés en paramètres à ce notebook.
La logique détaillée de la mise à jour des tables de la base de données est contenue dans le notebook Jupyter LAUNCHER.ipynb, le pipeline Python agissant
comme un orchestrateur qui valide les entrées et déclenche le processu de mise à jour.
"""


@pipeline("dse_update")
@parameter(
    "dse_file",
    name="DSE base de donnees (.csv)",
    type=File,
    required=False,  # ----------------------------------------------------------------------------------------------------------------------------------------------
    default=None,
    help="Selectionnez un fichier .csv dans le dossier correspondant contenant les donnees de base de la DSE.",
)
@parameter(
    "completude_file",
    name="Fichier de completude (.xlsx)",
    type=File,
    required=False,  # ----------------------------------------------------------------------------------------------------------------------------------------------
    default=None,
    help="Selectionnez un fichier .xlsx dans le dossier correspondant contenant les donnees de completude de la DSE.",
)
def dse_update(dse_file: File, completude_file: File):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    pipeline_path = Path(workspace.files_path) / "tdb-suivi-epidemio"
    current_run.log_info("Demarrage de la mise a jour de la base de donnees DSE...")

    dse_file = File(
        type="csv",
        name="2026_Database_Week05.csv",
        path=(pipeline_path / "data/raw/DSE_data_2026/2026_Database_Week05.csv").as_posix(),
        size=12,  # Replace with actual file size in bytes
    )
    completude_file = File(
        type="csv",
        name="2026_Database_Week05.csv",
        path=(pipeline_path / "data/raw/Completude/2026/COMPLETUDE PROMPTITUDE SURVEILLANCE_Week05.xlsx").as_posix(),
        size=12,  # Replace with actual file size in bytes
    )

    output_dir = pipeline_path / "papermill-outputs-V2"
    output_dir.mkdir(parents=True, exist_ok=True)
    validate_inputs(
        pipeline_path=pipeline_path,
        dse_file=Path(dse_file.path),
        completude_file=Path(completude_file.path),
        output_dir=output_dir,
    )

    run_notebook(
        nb_path=pipeline_path / "01_process_mapepi_dse_V2.ipynb",
        out_nb_path=output_dir,
        parameters={"nouvelles_donnees_path": dse_file.path},
    )

    run_notebook(
        nb_path=pipeline_path / "02_malaria_data_for_pnlp_tdb_V2.ipynb",
        out_nb_path=output_dir,
        parameters={"nouvelles_donnees_path": dse_file.path},
    )

    run_notebook(
        nb_path=pipeline_path / "03_completude_V2.ipynb",
        out_nb_path=output_dir,
        parameters={"nouvelle_completude_path": completude_file.path},
    )

    # run_notebook(
    #     nb_path=pipeline_path / "04_PNLP_dataset_update.ipynb",
    #     out_nb_path=output_dir,
    #     parameters={"nouvelles_donnees_path": dse_file.path},
    #     kernel_name="python3",
    # )

    # run_notebook(
    #     nb_path=pipeline_path / "05_COUSP_dataset_update.ipynb",
    #     out_nb_path=output_dir,
    #     parameters={"nouvelles_donnees_path": dse_file.path},
    # )

    # ---------- Superset updates ---------- #
    # run_notebook(
    #     nb_path=pipeline_path / "DSE_Surv_Epi_Complete_agg_update.ipynb",
    #     out_nb_path=output_dir,
    # )

    # run_notebook(
    #     nb_path=pipeline_path / "DSE_Surv_Epi_Complete_agg_ss_update.ipynb",
    #     out_nb_path=output_dir,
    # )


def validate_inputs(pipeline_path: Path, dse_file: Path, completude_file: Path, output_dir: Path):
    """Validate the existence and file type of the DSE and complétude files.

    Parameters
    ----------
    pipeline_path : Path
        Path to the pipeline directory, used for logging and output management.
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
    if not dse_file.exists() or dse_file.suffix.lower() != ".csv":
        current_run.log_error(f"Le fichier DSE doit être un fichier .csv valide. Chemin fourni: {dse_file}")
        raise ValueError
    if not completude_file.exists() or completude_file.suffix.lower() != ".xlsx":
        current_run.log_error(f"Le fichier de complétude doit être un fichier .xlsx valide. Chemin fourni: {completude_file}")
        raise ValueError

    notebooks_to_run = [
        {
            "notebook": pipeline_path / "00_validate_mapepi_csv_V2.ipynb",
            "params": {"nouvelles_donnees_path": dse_file.as_posix()},
            "file_name": dse_file.name,
            "kernel_name": "python3",
        },
        {
            "notebook": pipeline_path / "00_validate_completude_V2.ipynb",
            "params": {"nouvelles_completude_path": completude_file.as_posix()},
            "file_name": completude_file.name,
            "kernel_name": "ir",
        },
    ]

    for nb in notebooks_to_run:
        try:
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
    parameters: dict,
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
        current_run.log_error(f"Veuillez vérifier le notebook de sortie pour détecter d'eventuelles erreurs: {out_nb_full_path.name} ")
        raise
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'exécution du notebook: {nb_path.name}: {e}")
        raise


# def compute_output_name(nb_path: Path, out_nb_path: Path) -> Path:
#     """Generate an output file path for a notebook execution, appending a timestamp to the filename.

#     Parameters
#     ----------
#     nb_path : Path
#         Path to the input notebook file.
#     out_nb_path : Path
#         Directory where the output notebook will be saved.

#     Returns
#     -------
#     Path
#         Full path to the output notebook file with a timestamp in its name.
#     """
#     file_stem = nb_path.stem
#     extension = nb_path.suffix
#     execution_timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
#     return out_nb_path / f"{file_stem}_OUTPUT_{execution_timestamp}{extension}"


if __name__ == "__main__":
    dse_update()
