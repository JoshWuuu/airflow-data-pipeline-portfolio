from airflow.decorators import dag, task
import pendulum
import requests
import xmltodict
import os

from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"
EPISODE_FOLDER = "episodes"
FRAME_RATE = 16000

@dag(
    dag_id="podcast_summary",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 5, 1),
    catchup=False,
)
def podcast_summary():
    
    create_database = SqliteOperator(
        task_id='create_table_sqlite',
        sql=r"""
        CREATE TABLE IF NOT EXISTS episodes (
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT,
            transcript TEXT
        );
        """,
        sqlite_conn_id="podcasts"
    )

    @task()
    def get_episodes():
        data = requests.get(PODCAST_URL)
        feed = xmltodict.parse(data.text)
        eposides = feed["rss"]["channel"]["item"]
        print(f"Found {len(eposides)} episodes")
        return eposides
    
    podcast_episodes = get_episodes()
    create_database.set_downstream(podcast_episodes)

    @task()
    def load_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id="podcasts")
        stored_episodes = hook.get_pandas_df("SELECT * from episodes;")
        new_episodes = []
        for episode in episodes:
            if episode["link"] not in stored_episodes["link"].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode["link"], episode["title"], episode["pubDate"], episode["description"], filename])

        hook.insert_rows(table='episodes', rows=new_episodes, target_fields=["link", "title", "published", "description", "filename"])
        return new_episodes

    new_episodes = load_episodes(podcast_episodes)

    @task()
    def download_episodes(episodes):
        audio_files = []
        for episode in episodes:
            name_end = episode["link"].split('/')[-1]
            filename = f"{name_end}.mp3"
            audio_path = os.path.join(EPISODE_FOLDER, filename)
            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")
                # download the audio file
                audio = requests.get(episode["enclosure"]["@url"])
                with open(audio_path, "wb+") as f:
                    f.write(audio.content)
            audio_files.append({
                "link": episode["link"],
                "filename": filename
            })
        return audio_files

    audio_files = download_episodes(podcast_episodes)

podcast_summary_dag = podcast_summary()