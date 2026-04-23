import pandas as pd
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
# Extract
def extract(file_path):
    logging.info(f"Starting extraction from {file_path}")
    data=pd.read_csv(file_path)
    logging.info(f'Loaded {data.shape[0]} rows and {data.shape[1]} columns in this DataFrame.')
    logging.info(f"Columns: {list(data.columns)}")
    return data

# Transform
def transform(apps,reviews,category,min_rating,min_reviews):
    logging.info(f"Starting transformation for category={category}, "
                 f"min_rating={min_rating}, min_reviews={min_reviews}")
    reviews=reviews.drop_duplicates()
    apps=apps.drop_duplicates(['App'])
    subset_apps=apps.loc[apps['Category']==category]
    logging.info(f'Apps after category filter: {subset_apps.shape[0]} rows')
    subset_reviews=reviews.loc[reviews['App'].isin(subset_apps['App']),['App','Sentiment_Polarity']]
    aggregated_reviews = subset_reviews.groupby(by='App').mean()
    aggregated_reviews = aggregated_reviews.reset_index()
    joined_apps_review = pd.merge(
    subset_apps,
    aggregated_reviews,
    on="App",
    how="left")
    filtered_apps_review=joined_apps_review.loc[:,["App", "Rating", "Reviews", "Installs", "Sentiment_Polarity"]]
    # Convert Reviews to numeric and handle invalid values
    filtered_apps_review["Reviews"] = pd.to_numeric(filtered_apps_review["Reviews"], errors="coerce")
    # Drop rows where both Reviews and Rating are missing
    filtered_apps_review = filtered_apps_review.dropna(subset=["Reviews", "Rating"], how="all")
    top_apps=filtered_apps_review.loc[(filtered_apps_review['Rating']>min_rating) & (filtered_apps_review['Reviews']>min_reviews),:]
    top_apps=top_apps.sort_values(by=['Rating','Reviews'],ascending=False)
    top_apps.reset_index(drop=True,inplace=True)
    logging.info(f"Top apps after filtering: {top_apps.shape[0]} rows")
    top_apps.to_csv("top_apps1.csv")
    logging.info(f"Transformation complete: {top_apps.shape[0]} rows, {top_apps.shape[1]} columns and has been persisted")
    return top_apps

#Load
import sqlite3
def load(dataframe,database_name,table_name):
    con=sqlite3.connect(database_name)
    dataframe.to_sql(name=table_name,con=con,if_exists='replace',index=False)
    logging.info(f"Data loaded into table '{table_name}' in database '{database_name}'")
    loaded_dataframe=pd.read_sql(sql=f'SELECT * FROM {table_name}',con=con)
    logging.info('The loaded dataframe has been read from sql for validation')
    try:
        assert dataframe.shape==loaded_dataframe.shape
        logging.info(f'Data validation successful')
    except AssertionError:
        logging.error(f'Data validation failed: shape mismatch')
    con.close()
    
#Executing pipeline
def main():
    apps = extract("apps_data.csv")
    reviews = extract("review_data.csv")

    transformed = transform(apps, reviews, "FOOD_AND_DRINK", 4.0, 1000)

    load(transformed, "market_research.db", "top_apps")

if __name__ == "__main__":
    main()