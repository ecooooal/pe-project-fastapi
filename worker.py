async def worker():
    async for job in redis_subscriber():
        # Signal checking
        await notify_user(job["user_id"], "checking", {"answer_id": job["answer_id"]})

        # Pick executor
        executor_url = pick_executor(job["language"])

        # Run code
        result = await run_executor(executor_url, job["code"])

        # Save result
        db.update_answer(job["answer_id"], result)

        # Signal checked
        await notify_user(job["user_id"], "checked", {"answer_id": job["answer_id"]})
