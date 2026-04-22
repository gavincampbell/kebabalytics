"""
Bundle mutators for kebabalytics.

Mutators modify resources defined in YAML or Python at deployment time.
They run in the order specified in databricks.yml and apply to every
resource of the relevant type.
"""

from databricks.bundles.core import job_mutator
from databricks.bundles.jobs import Job, JobEmailNotifications


@job_mutator
def add_email_notifications(job: Job) -> Job:
    """Add failure notifications to any job that doesn't already have them."""
    if job.email_notifications is None:
        job.email_notifications = JobEmailNotifications(
            on_failure=["azure_demos@gavincampbell.dev"],
            no_alert_for_skipped_runs=True,
        )
    return job
