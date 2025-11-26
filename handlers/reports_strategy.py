from __future__ import annotations
from typing import Dict, Any, List
import polars as pl
from handlers.reports_interface import Context, Strategy
from utils.database_config import DATABASE_URL

class CalculateExamDescriptiveStatistics(Strategy):
    def calculate(self, df: pl.DataFrame) -> Dict[str, Any]:
        student_ids_list = df.select('user_id').unique().to_series().to_list()

        get_student_statuses_query = f"""
            WITH RankedAttempts AS (
                SELECT
                    er.attempt, er.status,
                    sp.user_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY sp.user_id
                        ORDER BY er.attempt DESC
                    ) as rn
                FROM
                    exam_records er
                JOIN
                    student_papers sp ON sp.id = er.student_paper_id
                WHERE
                    sp.user_id = ANY (ARRAY{student_ids_list}) 
            )
            SELECT
                *
            FROM
                RankedAttempts
            WHERE
                rn = 1
        """
        student_statuses_df = pl.read_database_uri(query=get_student_statuses_query, uri=DATABASE_URL)
        student_statuses_count = student_statuses_df.group_by('status').agg(pl.count().alias("count"))
        student_statuses_count_formatted = dict(zip(
            student_statuses_count.get_column("status").to_list(),
            student_statuses_count.get_column("count").to_list()
        ))

        student_scores = (
            df
            .group_by('user_id')
            .agg(
                pl.col('points_obtained').sum().alias('total_score'),
                pl.col('question_points').sum().alias('max_score')
            )
        )
        
        exam_summary_data = (
            student_scores
            .select(
                pl.col('total_score').mean().alias('mean'),
                pl.col('total_score').median().alias('median'),
                pl.col('total_score').std().round(2).alias('standard_deviation'), 
                pl.col('total_score').mode().alias('mode'), 
                pl.col('total_score').min().alias('min'),
                pl.col('total_score').max().alias('max'),
                pl.col('max_score').max().alias('exam_maximum_score')
            )
            .with_columns(
                (pl.col('max') - pl.col('min')).alias('range')
            )
        )
        exam_summary_data = exam_summary_data.row(0, named=True)
        
        raw_question_levels_summary_data = (
            df
            .group_by('question_level')
            .agg(
                pl.col('question_id').n_unique().alias('question_count'),
                pl.col('points_obtained').sum().alias('aggregated_students_score'),
                pl.col('question_points').sum().alias('aggregated_max_score'),
            )
            .with_columns(
                (pl.col("aggregated_students_score") / pl.col("aggregated_max_score")).round(2).alias("accuracy"),
                (pl.col("aggregated_students_score") / pl.col("aggregated_max_score") * 100).round(1).alias("accuracy_percentage")
            )
            .sort('accuracy_percentage', descending=True) 
        )
        list_of_level_dicts: List[Dict] = raw_question_levels_summary_data.to_dicts()
        question_levels_summary_data = {
            d['question_level']: {k: v for k, v in d.items() if k != 'question_level'}
            for d in list_of_level_dicts
        }

        raw_subjects_min_max_data = (
            df
            .group_by('subject_id')
            .agg(
                pl.col('subject_name').unique().first(),
                pl.col('question_id').n_unique().alias('question_count'),
                pl.col('points_obtained').sum().alias('aggregated_students_score'),
                pl.col('question_points').sum().alias('aggregated_max_score'),
            )
            .with_columns(
                (pl.col("aggregated_students_score") / pl.col("aggregated_max_score")).round(2).alias("accuracy"),
                (pl.col("aggregated_students_score") / pl.col("aggregated_max_score") * 100).round(1).alias("accuracy_percentage")
            )
            .sort('accuracy_percentage', descending=True)
        )

        top_three_max_subjects = (
            raw_subjects_min_max_data
            .sort('accuracy', descending=True)  # Sort descending for MAX
            .head(3)                            # Take the top 3
        ).to_dicts()

        # 3. Get Top 3 Minimum Accuracy Subjects
        top_three_min_subjects = (
            raw_subjects_min_max_data
            .sort('accuracy', descending=False) # Sort ascending for MIN
            .head(3)                             # Take the top 3 (which are the lowest)
        ).to_dicts()


        subjects_min_max_data = {
            "top_three_max_subjects": top_three_max_subjects,
            "top_three_min_subjects": top_three_min_subjects,
        }
        
        calculated_data = {
            'student_statuses_data' : student_statuses_count_formatted,
            'exam_summary_data' : exam_summary_data,
            'question_levels_summary_data' : question_levels_summary_data,
            'subjects_min_max_data' : subjects_min_max_data
        }
        
        return calculated_data
    
class CalculateExamOverview(Strategy):
    def calculate(self, df: pl.DataFrame) -> Dict[str, Any]:
        LEVEL_ORDER = [
            'remember',
            'understand',
            'apply',
            'analyze',
            'evaluate',
            'create'
        ]
        student_count = df['user_id'].n_unique()
        subjects = df['subject_id'].unique().to_list()
        subject_count = df['subject_id'].n_unique()
        courses = df['course_abbreviation'].unique().to_list()
        course_count = df['course_id'].n_unique()
        topic_count = df['topic_id'].n_unique()
        questions_count = df['question_id'].n_unique()
        question_levels = df['question_level'].unique().to_list()
        
        calculated_data = {
            'exam_overview_data' : {
                'student_count': student_count,
                'subjects': subjects,
                'subject_count' : subject_count,
                'courses': courses,
                'course_count' : course_count,
                'topic_count': topic_count,
                'question_count': questions_count,
                'questions_levels' : sorted(question_levels, key=LEVEL_ORDER.index)
            }
        }
        return calculated_data

class CalculateExamHistogramBoxplot(Strategy):
    def calculate(self, df: pl.DataFrame) -> Dict[str, Any]:
        exam_scores_histogram_box_plot = (
            df
            .group_by('user_id')
            .agg(
                pl.col('course_abbreviation').unique().first(),
                pl.col('points_obtained').sum().alias('total_score'),
                pl.col('question_points').sum().alias('max_score')
            )
        ).to_dicts()
        
        calculated_data = {
            'exam_histogram_boxplot_data' : exam_scores_histogram_box_plot
        }
        return calculated_data
    
class CalculateExamBySubjectsAndTopics(Strategy):
    def calculate(self, df: pl.DataFrame) -> Dict[str, Any]:
        df_subjects = self.calculate_normalized_scores(df, 'subject_name')
        exam_groupby_subjects = self.restructure_report(df_subjects, 'subject_name')

        df_topics = self.calculate_normalized_scores(df, 'topic_name')
        exam_groupby_topics = self.restructure_report(df_topics, 'topic_name')

        calculated_data = {
            'normalized_exam_scores_by_subjects': exam_groupby_subjects,
            'normalized_exam_scores_by_topics': exam_groupby_topics,
        }
        return calculated_data
    
    @staticmethod
    def calculate_normalized_scores(df: pl.DataFrame, group_col: str) -> pl.DataFrame:
        # Calculates the weighted normalized score grouped by course and a specified column.
        return (
            df
            .group_by('course_abbreviation', group_col)
            .agg(
                pl.col('points_obtained').sum().alias('total_score'),
                pl.col('question_points').sum().alias('total_max_score'),
            )
            .with_columns(
                (pl.col('total_score') / pl.col('total_max_score') * 100)
                .alias('normalized_score')
                .round(2) 
            )
            .select(['course_abbreviation', group_col, 'normalized_score'])
            .sort(['course_abbreviation', group_col])
        )
    
    @staticmethod
    def restructure_report(df_summary: pl.DataFrame, group_col: str) -> Dict[str, Dict[str, float]]:
        # {course: {group_col: score}} 
        report_list: List[Dict] = df_summary.to_dicts()
        final_report_data = {}
        
        for row in report_list:
            course = row['course_abbreviation']
            item = row[group_col]  
            score = row['normalized_score']
            
            if course not in final_report_data:
                final_report_data[course] = {}
            
            final_report_data[course][item] = score
            
        return final_report_data
    
class CalculateExamBYTypeWithLevels(Strategy):
    def calculate(self, df: pl.DataFrame) -> Dict[str, Any]:
        df_long = (
            df
            .group_by('course_abbreviation', 'question_type', 'question_level')
            .agg(
                pl.col('points_obtained').sum().alias('raw_score_sum'),
                pl.col('question_points').sum().alias('max_score_sum'),
            )
            .sort(['question_type', 'question_level', 'course_abbreviation'])
        )


        # --- B. Overall QType Aggregation (Only QType Raw Totals) ---
        df_qtype_totals = (
            df_long
            .group_by('course_abbreviation','question_type')
            .agg(
                pl.col('raw_score_sum').sum().alias('qtype_total_raw_score'),
                pl.col('max_score_sum').sum().alias('qtype_total_max_score'),
            )
        )

        # --- C. Join and Calculate Contribution % ---
        df_combined = (
            df_long
            .join(df_qtype_totals, on=['course_abbreviation', 'question_type'], how='left')
            .with_columns(
                # Normalized Score (Accuracy of this level)
                pl.when(pl.col('max_score_sum') > 0)
                .then(pl.col('raw_score_sum') / pl.col('max_score_sum') * 100)
                .otherwise(pl.lit(0.0))
                .round(1)
                .alias('accuracy_percentage'), # Renamed for clarity vs contribution
                
                # Contribution Percentage (The new required metric)
                pl.when(pl.col('qtype_total_raw_score') > 0)
                .then(pl.col('raw_score_sum') / pl.col('qtype_total_raw_score') * 100)
                .otherwise(pl.lit(0.0))
                .round(1)
                .alias('contribution_percentage')
            )
            .sort(['question_type', 'course_abbreviation',  'question_level'])
        )

        # --- D. Restructure ---
        restructured = self.restructure_for_plotly(df_combined)
        
        calculated_data = {
            'exam_by_types_with_levels' : restructured
        }
        return calculated_data
    
    @staticmethod
    def restructure_for_plotly(df_combined: pl.DataFrame) -> List[Dict[str, Any]]:
        type_map = {
            'multiple_choice': 'MCQ',
            'true_or_false': 'T/F',
            'identification': 'Identify',
            'ranking': 'Rank',
            'matching': 'Match',
            'coding' : 'Code'
        }
        # Group by Question Type and collect metrics into lists
        df_grouped_qtype = (
            df_combined
            .group_by(['course_abbreviation'])
            .agg(
                # Overall QType totals (should be constant across the group)
                pl.col('qtype_total_raw_score').implode(),
                pl.col('qtype_total_max_score').implode(),
                
                # Detailed data for the Bloom's breakdown (implode is correct here)
                pl.col('question_level').str.to_titlecase().implode().alias('levels'),
                pl.col('raw_score_sum').implode().alias('raw_scores'),
                pl.col('accuracy_percentage').implode().alias('accuracy_percentages'), 
                pl.col('contribution_percentage').implode().alias('contribution_percentages'), 
                pl.col('question_type').implode(),
            )
        )

        final_data = {}

        for row in df_grouped_qtype.iter_rows(named=True):
            course = row['course_abbreviation']
            question_type_data = {}

            # Process the Bloom's Level breakdown
            for qtype_name, qtype_raw, qtype_max, level, raw, acc, cont in zip(
                row['question_type'],
                row['qtype_total_raw_score'],
                row['qtype_total_max_score'],
                row['levels'], 
                row['raw_scores'], 
                row['accuracy_percentages'], 
                row['contribution_percentages'],
            ):
                if qtype_name not in question_type_data:
                    question_type_data[qtype_name] = {
                        'code' : type_map.get(qtype_name),
                        "qtype_raw_score_sum": qtype_raw,
                        "qtype_max_score_sum": qtype_max,
                        'blooms' : {}
                    }
                
                question_type_data[qtype_name]['blooms'][level] = {
                    'aggregated_raw_score': raw, 
                    'accuracy_percentage': acc, 
                    'contribution_percentage': cont ,

                }
            # Construct the final object
            custom_order = [
                "multiple_choice",
                "true_or_false",
                "identification",
                "ranking",
                "matching",
                'coding'
            ]

            # Reorder using a dictionary comprehension
            ordered_bscs_data = {key: question_type_data[key] for key in custom_order if key in question_type_data}

            final_data[course] = ordered_bscs_data
                
        return final_data
    
class CalculateExamQuestionHeatStrip(Strategy):
    def calculate(self, df: pl.DataFrame) -> Dict[str, Any]:
        exam_questions_score = (
            df
            .group_by('question_id')
            .agg(
                pl.col('question_name').unique().first(),
                pl.col('question_level').unique().first().str.to_titlecase(),
                pl.col('question_type').unique().first().str.replace_all("_", " ").str.to_titlecase(),
                pl.col('subject_name').unique().first(),
                pl.col('topic_name').unique().first(),
                pl.col('points_obtained').mean().alias('average_score'),
                pl.col('question_points').sum().alias('sum_of_question_points'),
                pl.col('question_points').unique().first().alias('maximum_points_attainable'),
                (pl.col("first_answered_at") - pl.col("first_viewed_at"))
                    .mean()
                    .dt.total_seconds()
                    .alias("average_time_to_answer"),
                (pl.col("last_answered_at") - pl.col("first_answered_at"))
                    .mean()
                    .dt.total_seconds()
                    .alias("average_time_to_reanswer")
            ).with_columns(
                pl.when(pl.col('sum_of_question_points') > 0)
                    .then(pl.col('average_score') / pl.col('sum_of_question_points') * 100)
                    .otherwise(pl.lit(0.0))
                    .round(1)
                    .alias('accuracy_percentage'),
            )
        ).to_dicts()
        
        calculated_data = {
            'exam_question_heatstrip' : exam_questions_score
        }
        return calculated_data
