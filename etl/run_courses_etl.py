from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.load import DatabaseLoader

extractor = DataExtractor()
transformer = DataTransformer()
loader = DatabaseLoader()

df = extractor.extract_csv("data/courses.csv")
df = transformer.transform_courses(df)
loader.load_courses(df)

print("Courses ETL completed")
