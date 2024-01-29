import os
import findspark
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

findspark.init()

# Initialize Spark session
spark = SparkSession.builder.appName("GitHubPRData").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)  # Property used to format output tables better


# Step 1: Extract - Get pull requests data and save to JSON files

def get_repo_prs(repo_name):
    pr_url = f'{repo_name}/pulls?q=is%3Apr'.replace('orgs/', '')
    response_prs = requests.get(pr_url)
    count = ''
    if response_prs.status_code == 200:
        soup = BeautifulSoup(response_prs.text, 'html.parser')
        count = len(soup.find_all(class_='js-issue-row'))

    return count


def get_merged_repo_prs(repo_name):
    pr_url = f'{repo_name}/pulls?q=is%3Aclosed'.replace('orgs/', '')
    response_prs = requests.get(pr_url)
    count = ''
    timestamp = ''
    if response_prs.status_code == 200:
        soup = BeautifulSoup(response_prs.text, 'html.parser')
        count = len(soup.find_all(class_='js-issue-row'))
        opened_by_div = soup.select_one('.opened-by')
        if opened_by_div:
            relative_time_element = opened_by_div.select_one('relative-time')
            timestamp = relative_time_element['datetime']

    return count, timestamp


def scrape_repo_info(html_content: tuple):
    soup = BeautifulSoup(html_content[0].text, 'html.parser')
    repo_list = []
    access_token = "YOUR_GITHUB_TOKEN"

    for repo in soup.find_all('li', class_='Box-row'):
        repo_info = {
            'repository_name': repo.find('a', itemprop='name codeRepository').get_text(strip=True),
            'repository_id': 'https://github.com' + repo.find('a', itemprop='name codeRepository')['href'],
            'Organization_Name': html_content[1].split('/')[-1]
        }
        repo_info['num_prs'] = get_repo_prs(f"{html_content[1]}/{repo_info['repository_name']}")
        merged_data = get_merged_repo_prs(f"{html_content[1]}/{repo_info['repository_name']}")
        repo_info['num_prs_merged'] = merged_data[0]
        repo_info['merged_at'] = merged_data[1]
        repo_info['repository_owner'] = ''

        api_url = f"https://api.github.com/orgs/{repo_info['Organization_Name']}/repos"
        response = requests.get(api_url, headers={"Authorization": f"token {access_token}"})

        # Parse the JSON response and get the owner
        repo_details = response.json()
        for i in repo_details:
            get_repo = i['name']
            if repo_info['repository_name'].lower() == get_repo.lower():
                repo_info['repository_owner'] = i["owner"]["login"]

        if (repo_info['num_prs'] == repo_info['num_prs_merged']) and (
                "scytale" in repo_info['repository_owner'].lower()):
            compliant = True
        else:
            compliant = False

        repo_info['is_compliant'] = compliant

        repo_list.append(repo_info)

    return repo_list


def main(github_url):
    response_repos = requests.get(f'{github_url}/repositories')
    response_repos = (response_repos, github_url)

    if response_repos[0].status_code == 200:
        # Use map to apply the scraping logic to each repository HTML content
        repo_info_list = list(
            filter(None, spark.sparkContext.parallelize([response_repos]).map(scrape_repo_info).collect()))

        # Save pull request data to JSON files
        for idx, repo_info in enumerate(repo_info_list):
            spark.createDataFrame(repo_info).write.json(f"extracted_data/repo_{idx}.json", mode='overwrite')
    else:
        print(f"Failed to fetch repository data. Status code: {response_repos.status_code}")

    # Define schema for the table
    schema = StructType([
        StructField("Organization_Name", StringType(), True),
        StructField("repository_id", StringType(), True),
        StructField("repository_name", StringType(), True),
        StructField("repository_owner", StringType(), True),
        StructField("num_prs", IntegerType(), True),
        StructField("num_prs_merged", IntegerType(), True),
        StructField("merged_at", StringType(), True),
        StructField("is_compliant", StringType(), True),
    ])

    # Read JSON files and create DataFrame with the specified schema
    transformed_data = spark.read.schema(schema).json("extracted_data/*.json")

    # Save transformed data to Parquet file
    transformed_data.write.parquet("transformed_data.parquet", mode='overwrite')

    return True


if __name__ == '__main__':
    github_url = 'https://github.com/orgs/Scytale-exercise'
    response_repos = requests.get(f'{github_url}/repositories')

    # call main
    main(github_url)

    # Read from the Parquet file
    transformed_data_read = spark.read.parquet("transformed_data.parquet")
    transformed_data_read.show()

    # Stop Spark session
    spark.stop()
