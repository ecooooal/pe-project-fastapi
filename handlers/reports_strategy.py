from __future__ import annotations
from typing import Dict, Any, List
import polars as pl
from app.handlers.reports_interface import Context, Strategy

class CalculateExamDescriptiveStatistics(Strategy):
    def calculate(self, df: pl.DataFrame) -> Dict[str, Any]:
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
                (pl.col("aggregated_students_score") / pl.col("aggregated_max_score")).round(2).alias("overall_ratio"),
                (pl.col("aggregated_students_score") / pl.col("aggregated_max_score") * 100).round(1).alias("overall_percentage")
            )
            .sort('overall_percentage', descending=True) 
        )
        list_of_level_dicts: List[Dict] = raw_question_levels_summary_data.to_dicts()
        question_levels_summary_data = {
            d['question_level']: {k: v for k, v in d.items() if k != 'question_level'}
            for d in list_of_level_dicts
        }

        subjects_min_max_data = (
            df
            .group_by('subject_id')
            .agg(
                pl.col('subject_name').unique().first(),
                pl.col('question_id').n_unique().alias('question_count'),
                pl.col('points_obtained').sum().alias('aggregated_students_score'),
                pl.col('question_points').sum().alias('aggregated_max_score'),
            )
            .with_columns(
                (pl.col("aggregated_students_score") / pl.col("aggregated_max_score")).round(2).alias("overall_ratio"),
                (pl.col("aggregated_students_score") / pl.col("aggregated_max_score") * 100).round(1).alias("overall_percentage")
            )
            .sort('overall_percentage', descending=True)
        ).to_dicts()

        calculated_data = {
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
        courses = df['course_abbreviation'].unique().to_list()
        topics_count = df['topic_id'].n_unique()
        questions_count = df['question_id'].n_unique()
        question_levels = df['question_level'].unique().to_list()
        
        calculated_data = {
            'exam_overview_data' : {
                'students_count': student_count,
                'subjects': subjects,
                'courses': courses,
                'topics_count': topics_count,
                'questions_count': questions_count,
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
        """Calculates the weighted normalized score grouped by course and a specified column."""
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
        """Converts the Polars summary DataFrame into the nested {course: {group_col: score}} dictionary."""
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
            .group_by('question_type')
            .agg(
                pl.col('raw_score_sum').sum().alias('qtype_total_raw_score'),
                pl.col('max_score_sum').sum().alias('qtype_total_max_score'),
            )
        )

        # --- C. Join and Calculate Contribution % ---
        df_combined = (
            df_long
            .join(df_qtype_totals, on='question_type', how='left')
            .with_columns(
                # Normalized Score (Accuracy of this level)
                pl.when(pl.col('max_score_sum') > 0)
                .then(pl.col('raw_score_sum') / pl.col('max_score_sum') * 100)
                .otherwise(pl.lit(0.0))
                .alias('accuracy_percentage'), # Renamed for clarity vs contribution
                
                # Contribution Percentage (The new required metric)
                pl.when(pl.col('qtype_total_raw_score') > 0)
                .then(pl.col('raw_score_sum') / pl.col('qtype_total_raw_score') * 100)
                .otherwise(pl.lit(0.0))
                .alias('contribution_percentage')
            )
        )

        # --- D. Restructure ---
        restructured = self.restructure_for_plotly(df_combined)
        
        calculated_data = {
            'exam_by_types_with_levels' : restructured
        }
        return calculated_data
    
    @staticmethod
    def restructure_for_plotly(df_combined: pl.DataFrame) -> List[Dict[str, Any]]:
        # Get the list of courses (X-axis order)
        courses = df_combined['course_abbreviation'].unique().sort().to_list()
        
        # Group by Question Type and collect metrics into lists
        df_grouped_qtype = (
            df_combined
            .group_by(['question_type'])
            .agg(
                # Overall QType totals (should be constant across the group)
                pl.col('qtype_total_raw_score').first().alias('qtype_total_raw_score'),
                pl.col('qtype_total_max_score').first().alias('qtype_total_max_score'),
                
                # Detailed data for the Bloom's breakdown (implode is correct here)
                pl.col('question_level').implode().alias('levels'),
                pl.col('raw_score_sum').implode().alias('raw_scores'),
                pl.col('accuracy_percentage').implode().alias('accuracy_percentages'), # Renamed
                pl.col('contribution_percentage').implode().alias('contribution_percentages'), # NEW
                pl.col('course_abbreviation').implode().alias('courses_list')
            )
        )

        final_data = []
        
        for row in df_grouped_qtype.to_dicts():
            qtype_name = row['question_type']
            blooms_data = {}
            
            # Overall QType scores
            qtype_raw = row['qtype_total_raw_score']
            qtype_max = row['qtype_total_max_score']
            
            # Process the Bloom's Level breakdown
            for level, raw, acc, cont, course_abbr in zip(
                row['levels'], 
                row['raw_scores'], 
                row['accuracy_percentages'], 
                row['contribution_percentages'], # Use the new contribution list
                row['courses_list']
            ):
                if level not in blooms_data:
                    # Initialize lists with 0.0 for all courses
                    blooms_data[level] = {
                        'raw': [0.0] * len(courses), 
                        'accuracy': [0.0] * len(courses), # Accuracy % of this level
                        'contribution': [0.0] * len(courses) # Contribution % (NEW)
                    }
                
                course_index = courses.index(course_abbr)
                
                # Populate the fixed-length lists at the correct course index
                blooms_data[level]['raw'][course_index] = raw
                blooms_data[level]['accuracy'][course_index] = acc
                blooms_data[level]['contribution'][course_index] = cont # Populate contribution

            # Construct the final object
            final_data.append({
                "name": qtype_name,
                "raw_score_sum": qtype_raw, # Total points scored for this QType across all courses/levels
                "max_score_sum": qtype_max, # Total max points for this QType across all courses/levels
                "blooms": blooms_data
            })
                
        return final_data
class CalculateExamQuestionHeatStrip(Strategy):
    def calculate(self, df: pl.DataFrame) -> Dict[str, Any]:
        exam_questions_score = (
            df
            .group_by('question_level', 'question_name',)
            .agg(
                pl.col('question_type').unique().first(),
                pl.col('subject_name').unique().first(),
                pl.col('topic_name').unique().first(),
                pl.col('points_obtained').mean().alias('average_score'),
                pl.col('question_points').sum().alias('max_score'),
            )
            .with_columns(
                pl.when(pl.col('max_score') > 0)
                .then(pl.col('average_score') / pl.col('max_score') * 100)
                .otherwise(pl.lit(0.0))
                .alias('accuracy_percentage')
            )
        ).to_dicts()
        
        calculated_data = {
            'exam_question_heatstrip' : exam_questions_score
        }
        return calculated_data
