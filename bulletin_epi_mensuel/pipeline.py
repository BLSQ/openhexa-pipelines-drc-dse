import papermill as pm
from openhexa.sdk import current_run, pipeline

@pipeline("bulletin-epi-mensuel", name="Bulletin épidémiologique mensuel")
@parameter(
    "get_annee",
    name="Year",
    help="",
    type=int,
    default=2023,
    required=True,
)
@parameter(
    "get_week_start",
    name="Week start",
    help="",
    type=int,
    default=4,
    required=True,
)
@parameter(
    "get_week_end",
    name="Week end",
    help="",
    type=int,
    default=9,
    required=True,
)
@parameter(
    "get_month_name",
    name="Month name",
    help="",
    type=str,
    default="Février",
    required=True,
)
@parameter(
    "get_month_num",
    name="Month number",
    help="",
    type=int,
    default=2,
    required=True,
)
def bulletin_epi_mensuel(
    get_annee, 
    get_week_start,
    get_week_end,
    get_month_name,
    get_month_num 
):
    
    # setup variables
    PROJ_ROOT = f'{workspace.files_path}/bulletin-epi-mensuel/'

    INPUT_NB = f'{PROJ_ROOT}LAUNCHER_DSE_bulletin.ipynb'
    OUTPUT_NB_DIR = f'{PROJ_ROOT}papermill-outputs/'

    params = {
        'annee': get_annee, 
        'week_start': get_week_start,
        'week_end': get_week_end, 
        'month_name': get_month_name,
        'month_num': get_month_num,
    }

    ppml = run_papermill_script(INPUT_NB, OUTPUT_NB_DIR, params)


@bulletin_epi_mensuel.task
def run_papermill_script(in_nb, out_nb_dir, parameters, *args, **kwargs):

    execution_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H:%M:%S")
    out_nb = f"{out_nb_dir}{os.path.basename(in_nb)}_OUTPUT_{execution_timestamp}.ipynb"

    pm.execute_notebook(in_nb, out_nb, parameters)
    return   

if __name__ == "__main__":
    bulletin_epi_mensuel()
