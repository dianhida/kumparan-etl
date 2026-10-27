Data Engineer Assessment V3
kumparan is a growing online media company. kumparan was first announced on 2016, and now
the company has millions of daily active users (and hopefully will increase time over time). The
number of contents produced in kumparan also growing, from hundred of thousands to more
than 1 million a day.
In a growing media company, Business Intelligence is a must, to provide analysis and
recommendation. Therefore, one of the importance of a Data Engineer, is to provide the data
that is ready for analytical purpose.
One of the most critical data in kumparan is articles.

Your Task
1. Create an ETL/ELT from a Database (SQL or NoSQL) to a Data Warehouse (choose one you
are most familiar with). You could use Mage, Airflow, or any data pipeline tool that you know.
2. The ETL/ELT will run each hour everyday.
Bonus
1. The ETL/ELT that you made is new, and the data that is in the database is from 2016. What
should you consider?
2. What if the table design is using a `hard delete` method. That means once the data is deleted,
the row is also get deleted. So the data in the data warehouse and in the database should
sync.
Note on AI Usage
We recognize that AI tools (such as ChatGPT, Gemini, Claude, Copilot, etc.) are increasingly
used in technical work and exploration. Therefore, we allow the use of AI to help complete this
assessment, with the following important conditions:
1 *articles*
2
3 - id
4 - title
5 - content
6 - published_at
7 - author_id
8 - created_at // filled when the article is created
9 - updated_at // updated when the article is created or is updated
10 - deleted_at // filled when the article got deleted

Transparency is required: Please clearly indicate which parts of your work were assisted by
AI. For example, whether AI was used for idea exploration, initial code generation,
documentation writing, or other tasks.
You are fully responsible: You remain accountable for the quality and understanding of the
solution you submit. We will evaluate not just the final result, but also your grasp of the
approach you used.
We’ll know: Our team has also tested this assessment using various AI tools. So we’ll be able
to tell if you simply copied AI output without meaningful thought or adaptation.
The main goal of this assessment is to understand your thinking and problem-solving approach.
AI can assist, but it cannot replace your own critical thinking and understanding.
Timeline & Expectations
You have 3 days to complete this assessment. While AI tools might help you finish faster, our
priority is quality over speed. Use the time wisely to refine your approach, validate your results,
and ensure your submission reflects your own thinking and understanding.
The goal of this assessment is to understand how you think and solve problems, not just how
fast you can generate output.