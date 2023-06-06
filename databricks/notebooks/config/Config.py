# Databricks notebook source
class Config():
    
    control_db = 'control'
    orchestrator_table_name = 'orchestrator'
    db = 'songs'

    # dict of the steps make sure the name is the name of the function that needs to be executed
    orch_steps = {
        1:{
            'id':'1',
            'name':'import_data',
            'parameters': f'{db}'
        },
        2:{
            'id':'2',
            'name':'cleanup_data',
            'parameters': f'{db}'
        },
        3:{
            'id':'3',
            'name':'calculate_times_played',
            'parameters': f'{db}'
        },
        4:{
            'id':'4',
            'name':'generate_top_10',
            'parameters': f'{db}'
        },
        5:{
            'id':'5',
            'name':'generate_bottem_10',
            'parameters': f'{db}'
        }
    }
