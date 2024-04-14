"""
ByteGenie API agent examples
"""

from utils.byte_genie import ByteGenie

## initiate ByteGenie
bg = ByteGenie(
    task_mode='sync',
)
"""
Company revenue comparison example
"""
## run a prompt for comparing company revenue
resp = bg.execute(
    prompt="I want to compare the difference in revenue segments of Apple, Google, Microsoft"
)
## get resp recipe (sequence of API calls)
recipe = resp.get_recipe()
## get resp output
output = resp.get_output()
## checkout output recipe (sequence of API calls)
recipe.keys()
recipe['task_1']
recipe['data_check']
recipe['task_2']
recipe['task_3']
## check output data
output.keys()
output['task_1']
output['task_2']
output['task_3']
"""
Notion tasks example
"""
## run a prompt for comparing company revenue
resp = bg.execute(
    prompt="I want to analyse all the tech related tasks on Notion to calculate distribution of tasks across my team for the next two weeks. Specifically, I want to know\n - how many tasks each person is assigned;\n - what are the total number of tasks by task priority;\n - what are the number of tasks per person by priority."
)
## get resp recipe (sequence of API calls)
recipe = resp.get_recipe()
## get resp output
output = resp.get_output()
## checkout output recipe (sequence of API calls)
recipe.keys()
recipe['task_1']
recipe['task_2']
recipe['task_3']
recipe['task_4.1']
recipe['task_4.2']
recipe['task_5']
## check output data
output.keys()
output['task_1'].to_dict('records')
output['task_2'].to_dict('records')
output['task_3'].to_dict('records')
output['task_4.1'].to_dict('records')
output['task_4.2'].to_dict('records')
output['task_5']
