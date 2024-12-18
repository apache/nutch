import boto3
import time


S3_BUCKET = 'debank-temp'


StepConfig = {
    'injector': {
        'Name': 'nutch-injector',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': f's3://{S3_BUCKET}/nutch/apache-nutch-1.18.job',
            'Args': [
                'org.apache.nutch.crawl.Injector',
                f's3://{S3_BUCKET}/nutch/crawldb',
                f's3://{S3_BUCKET}/nutch/urls'
            ]
        }
    },
    'generator': {
        'Name': 'nutch-generator',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': f's3://{S3_BUCKET}/nutch/apache-nutch-1.18.job',
            'Args': [
                'org.apache.nutch.crawl.Generator',
                f's3://{S3_BUCKET}/nutch/crawldb',
                f's3://{S3_BUCKET}/nutch/crawldb/segments',
                '-topN',
                '1000'
            ]
        }
    },
    'fetcher': {
        'Name': 'nutch-fetcher',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': f's3://{S3_BUCKET}/nutch/apache-nutch-1.18.job',
            'Args': [
                'org.apache.nutch.fetcher.Fetcher',
                # 's3://{S3_BUCKET}/nutch/segments/{LAST_SEGMENT}',
            ]
        }
    },
    'parse': {
        'Name': 'nutch-parse',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': f's3://{S3_BUCKET}/nutch/apache-nutch-1.18.job',
            'Args': [
                'org.apache.nutch.parse.ParseSegment',
                # 's3://{S3_BUCKET}/nutch/segments/{LAST_SEGMENT}',
            ]
        }
    },
    'updatedb': {
        'Name': 'nutch-updatedb',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': f's3://{S3_BUCKET}/nutch/apache-nutch-1.18.job',
            'Args': [
                'org.apache.nutch.crawl.CrawlDb',
                f's3://{S3_BUCKET}/nutch/crawldb',
                # 's3://{S3_BUCKET}/nutch/segments/{LAST_SEGMENT}',
            ]
        }
    },
    'invertlinks': {
        'Name': 'nutch-invertlinks',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3://debank-temp/nutch/apache-nutch-1.18.job',
            'Args': [
                'org.apache.nutch.crawl.LinkDb',
                f's3://{S3_BUCKET}/nutch/linkdb',
                '-dir',
                f's3://{S3_BUCKET}/nutch/crawldb/segments',
            ]
        }
    },
}

def get_largest_segment(bucket_name, prefix):
    """
    获取 S3 前缀下数字序列最大的目录路径。

    :param bucket_name: S3 存储桶名称
    :param prefix: 对象前缀（例如 'nutch/segments/'）
    :return: 最大的目录路径（完整 S3 Key）
    """
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter='/'
    )

    # 获取目录列表
    directories = response.get('CommonPrefixes', [])
    if not directories:
        print("No directories found under the specified prefix.")
        return None

    # 提取目录名称并排序
    segments = [
        int(d['Prefix'].rstrip('/').split('/')[-1])  # 提取最后的数字部分
        for d in directories
    ]
    largest_segment = max(segments)  # 获取最大的数字

    # 返回完整路径
    return f"{prefix}{largest_segment}/"


# 提交步骤并等待步骤完成
def submit_and_wait_for_step(cluster_id, step_id, step_config):
    emr = boto3.client('emr')

    if step_id in {'fetcher', 'parse', 'updatedb'}:
        last_segment = get_largest_segment(S3_BUCKET, 'nutch/crawldb/segments/')
        if last_segment is None:
            print(f"Step {step_id} skipped due to missing segment.")
            return 'FAILED'
        last_segment_arg = f's3://{S3_BUCKET}/{last_segment}'
        print(f"Using segment: {last_segment_arg}")
        step_config['HadoopJarStep']['Args'].append(last_segment_arg)

    # 提交步骤
    response = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[step_config]
    )

    step_id = response['StepIds'][0]
    print(f"Submitted step ID: {step_id} {step_config['Name']}")

    # 等待步骤完成
    while True:
        result = emr.describe_step(
            ClusterId=cluster_id,
            StepId=step_id
        )
        state = result['Step']['Status']['State']

        print(f"Current state: {state}")

        if state in ['COMPLETED', 'FAILED', 'CANCELLED']:
            print(f"Step finished with state: {state}")
            if state in {'FAILED', 'CANCELLED'}:
                print(f"Reason: {result['Step']['Status']['FailureDetails']['Reason']}")
            return state

        time.sleep(10)  # 每30秒检查一次状态

# 使用方法
if __name__ == '__main__':
    cluster_id = 'j-QJME3WHBVL9I'

    for i in range(2):
        steps = ['generator', 'fetcher', 'parse', 'updatedb']
        for step_id in steps:
        # for step_id, step_config in StepConfig.items():
            step_config = StepConfig[step_id]
            result = submit_and_wait_for_step(cluster_id, step_id, step_config)
            if result != 'COMPLETED':
                print(f"Step {step_id} failed")
                break
    print(f"Step {step_id} completed")

