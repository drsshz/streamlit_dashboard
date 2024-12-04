from controllers.superstore_controller import SuperstoreController
from etl.etl_pipeline import run_etl_pipeline

if __name__ == "__main__":
    superstore_app = SuperstoreController()
    superstore_app.run(run_etl_pipeline())
