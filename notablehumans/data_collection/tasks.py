from celery import shared_task


@shared_task()
def get_users_count():
    """A pointless Celery task to demonstrate usage."""
    return "this is a task"
