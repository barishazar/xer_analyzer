import pandas as pd
import os
import numpy as np
import openpyxl
#import xlsxwriter
from pandas import ExcelWriter
from graphviz import Digraph
from graphviz import Source
from datetime import datetime

#DEFINE A CLEANER FUNCTION FOR EACH XER IMPORTED


#print('Enter daily working hours as per project calendar:')



def ImportedFiles(imported_files,path_to_save_files):
    daily_working_hrs = 8
    path_saving=path_to_save_files
    files=imported_files
    #return path_saving, files


    my_path=path_saving


    #   Clean
    def XerCleaner(xer_file_to_be_cleaned,file_name):
        
        xer_file_to_be_cleaned['XER_FILE_NAME']=file_name #   Add File Name as a separate column
        xer_file_to_be_cleaned.loc[xer_file_to_be_cleaned[0]=="%T",'TABLE_LOC']=xer_file_to_be_cleaned[1] #Mark location of each table
        xer_file_to_be_cleaned['TABLE_LOC'].fillna(method='ffill', inplace=True) #Fill down table names
        task_table=xer_file_to_be_cleaned #Copy the dataframe
        
        fltr=(xer_file_to_be_cleaned['TABLE_LOC']=="TASKPRED") & (xer_file_to_be_cleaned[1]!="TASKPRED")
        fltr_task=(xer_file_to_be_cleaned['TABLE_LOC']=="TASK") & (xer_file_to_be_cleaned[1]!="TASK")

        xer_file_to_be_cleaned=xer_file_to_be_cleaned[fltr]
        task_table=task_table[fltr_task]
    
        xer_file_to_be_cleaned=xer_file_to_be_cleaned.reset_index()
        task_table=task_table.reset_index()

        xer_file_to_be_cleaned.columns= xer_file_to_be_cleaned.iloc[0]
        xer_file_to_be_cleaned=xer_file_to_be_cleaned.reset_index()

        task_table.columns= task_table.iloc[0]
        task_table=task_table.reset_index()
        
        xer_file_to_be_cleaned.dropna(axis='columns', inplace=True, how="all")
        xer_file_to_be_cleaned =xer_file_to_be_cleaned.drop(labels=0, axis=0)

        task_table.dropna(axis='columns', inplace=True, how="all")

        def Start(ac_st, ea_st):
            
        
        
            if str(ac_st)!='nan':
                if ac_st=='act_start_date':
                    Start=ac_st
                else:
                    Start=datetime.strptime(ac_st,'%Y-%m-%d %H:%M')

            else:

                Start=datetime.strptime(ea_st,'%Y-%m-%d %H:%M')

            return Start


        
        def Finish(ac_fn, ea_fn):
            if str(ac_fn)!='nan':
                if ac_fn=='act_end_date':
                    Finish=ac_fn
                else:
                    Finish=datetime.strptime(ac_fn,'%Y-%m-%d %H:%M')
            else:

                Finish=datetime.strptime(ea_fn,'%Y-%m-%d %H:%M')
            
            return Finish


        task_table['Start']=task_table.apply(lambda x: Start(x.act_start_date,x.early_start_date), axis=1)
        task_table['Finish']=task_table.apply(lambda x: Finish(x.act_end_date,x.early_end_date), axis=1)
        evolution_table=task_table[['task_code','task_name','phys_complete_pct','status_code','total_float_hr_cnt', 'target_drtn_hr_cnt', 'remain_drtn_hr_cnt', 'Start', 'Finish']]
        task_table =task_table.drop(labels=0, axis=0)
        

        number_of_columns=len(xer_file_to_be_cleaned.columns)
        #print(number_of_columns)
        pd.set_option('display.max_columns',74)

        xer_file_to_be_cleaned.rename(columns={ xer_file_to_be_cleaned.columns[number_of_columns-1]: "TABLE_LOC" }, inplace = True)
        xer_file_to_be_cleaned.rename(columns={ xer_file_to_be_cleaned.columns[number_of_columns-2]: "XER_FILE_NAME" }, inplace = True)

        xer_file_to_be_cleaned=xer_file_to_be_cleaned[['task_id','pred_task_id','pred_type','lag_hr_cnt','XER_FILE_NAME']]
        task_table=task_table[['task_id','task_code']]
        
        
        xer_file_to_be_cleaned=xer_file_to_be_cleaned.dropna(axis=0,how='all')

        pred_table_first_merge=pd.merge(xer_file_to_be_cleaned,task_table,left_on='task_id',right_on='task_id', how='left')
        pred_table_first_merge.rename(columns={ 'task_code': "Successor" }, inplace = True)
        pred_table_second_merge=pd.merge(pred_table_first_merge,task_table,left_on='pred_task_id',right_on='task_id', how='left')
        pred_table_second_merge.rename(columns={ 'task_code': "Predecessor" }, inplace = True)

        final_pred_table=pred_table_second_merge[['Predecessor','Successor','pred_type','lag_hr_cnt','XER_FILE_NAME']]
        task_table['XER_FILE_NAME']=file_name 
        evolution_table['XER_FILE_NAME']=file_name 
        cleaned_xer=final_pred_table

        #master_xer_output=MasterDataFrame(cleaned_xer)

        return cleaned_xer,task_table,evolution_table



    def GenerateGraph(graph_df, graph_name, node_clr_att):
        dot = Digraph('unix',filename='unix.gv',node_attr={'color': 'yellow', 'style': 'filled'})
        dot.graph_attr.update({'rankdir':'LR'})
        for index, row in graph_df.iterrows():
            a = row['Predecessor']
            b = row['Successor']
            link=row['XER_FILE_NAME'][:2]+ '-'+ row['Change'][:2] +'-'+ row['pred_type'][-2:]  +' '+ str(row['lag_hr_cnt'])

            #g.addEdge(a, b)
            dot.edge(str(a),str(b),link)

        for index, row in node_clr_att.iterrows():
            a = row['Change']
            b= row['task_code']

            if a=='ADDED ACTIVITY':
                dot.node(b,_attributes={'color': 'green'})
            else:
                dot.node(b,_attributes={'color': 'lightpink'})




        dot.render(my_path+'/'+graph_name, view=True)


    #IMPORT THE FILES


    master_xer=pd.DataFrame()
    master_xer_task=pd.DataFrame()
    master_xer_evolution=pd.DataFrame()
    for file in files:
        #xer_file=pd.read_csv(path+"/"+file,sep='\t',names=range(150), encoding= 'unicode_escape',dtype=str)
        xer_file = pd.read_csv(file, sep='\t', names=range(150), encoding='unicode_escape', dtype=str)
        imported_files_name=(file.filename)
        consolidated_pred_table,consolidated_task_table,consolidated_evolution_table=XerCleaner(xer_file,imported_files_name)
        
        
        master_xer=pd.concat([master_xer,consolidated_pred_table])
        master_xer_task=pd.concat([master_xer_task,consolidated_task_table])
        master_xer_evolution=pd.concat([master_xer_evolution,consolidated_evolution_table])

        master_xer['min_key']=master_xer['Predecessor']+'|'+master_xer['Successor']
        master_xer['mid_key']=master_xer['Predecessor']+'|'+master_xer['Successor']+'|'+master_xer['pred_type']
        master_xer['max_key']=master_xer['Predecessor']+'|'+master_xer['Successor']+'|'+master_xer['pred_type']+'|'+master_xer['lag_hr_cnt']
        master_xer['fixed_index']='A'


        master_xer_task['min_key']=master_xer_task['task_code']
        master_xer_task['fixed_index']='A'
        


    master_xer=master_xer.dropna(axis=0)
    master_xer_task=master_xer_task.dropna(axis=0)
    master_xer_evolution=master_xer_evolution.dropna(axis=0, how='all')



    #GET THE FILE NAMES IN AN ARRAY

    total_file_names=master_xer['XER_FILE_NAME'].unique()
    filecount=len(total_file_names)
    #print(total_file_names)


    def Bl_ApplyStatus(final_key):
        bl_stat_dict={"N-N-N":"DELETED RELATIONSHIP", "A-A-N":"LAG MODIFIED", "A-N-N":"DELETED LINK", "A-A-A":"NO CHANGE"}
        rl_bl_status=bl_stat_dict[final_key]
        return rl_bl_status

    def Up_ApplyStatus(final_key):
        up_stat_dict={"N-N-N":"ADDED RELATIONSHIP", "A-A-N":"LAG MODIFIED", "A-N-N":"ADDED LINK", "A-A-A":"NO CHANGE"}
        rl_up_status=up_stat_dict[final_key]
        return rl_up_status



    def Bl_ApplyStatusTask(final_key):
        bl_stat_dict_task={"N":"DELETED ACTIVITY", "A":"NO CHANGE"}
        tk_bl_status=bl_stat_dict_task[final_key]
        return tk_bl_status

    def Up_ApplyStatusTask(final_key):
        up_stat_dict_task={"N":"ADDED ACTIVITY", "A":"NO CHANGE"}
        tk_up_status=up_stat_dict_task[final_key]
        return tk_up_status


    def ActStatus(status_input):
        act_stat_dict={'TK_NotStart':'Not Started', 'TK_Complete':'Completed', 'TK_Active':'In Progress'}
        status_val=act_stat_dict[status_input]
        return status_val





    #GENERATE SUB-DATAFRAMES AND COMPARE THEM EACHOTHER
    export_scope_master=pd.DataFrame()
    export_scope_task_master=pd.DataFrame()

    for i in range(filecount-1):
        bl_filter=master_xer['XER_FILE_NAME']==total_file_names[i]
        up_filter=master_xer['XER_FILE_NAME']==total_file_names[i+1]
        bl_dataframe=master_xer[bl_filter]
        up_dataframe=master_xer[up_filter]

        bl_filter_task=master_xer_task['XER_FILE_NAME']==total_file_names[i]
        up_filter_task=master_xer_task['XER_FILE_NAME']==total_file_names[i+1]
        bl_dataframe_task=master_xer_task[bl_filter_task]
        up_dataframe_task=master_xer_task[up_filter_task]



        bl_dataframe_min_merge=pd.merge(bl_dataframe,up_dataframe[['min_key','fixed_index']],left_on='min_key',right_on='min_key', how='left')  
        bl_dataframe_mid_merge=pd.merge(bl_dataframe_min_merge,up_dataframe[['mid_key','fixed_index']],left_on='mid_key',right_on='mid_key', how='left')    
        bl_dataframe_max_merge=pd.merge(bl_dataframe_mid_merge,up_dataframe[['max_key','fixed_index']],left_on='max_key',right_on='max_key', how='left')   
        bl_dataframe_min_merge_task=pd.merge(bl_dataframe_task,up_dataframe_task[['min_key','fixed_index']],left_on='min_key',right_on='min_key', how='left')

        bl_tbl_size=len(bl_dataframe_max_merge.columns)
        bl_tbl_size_task=len(bl_dataframe_min_merge_task.columns)
        bl_dataframe_max_merge=bl_dataframe_max_merge.fillna('N')
        bl_dataframe_min_merge_task=bl_dataframe_min_merge_task.fillna('N')

        bl_dataframe_max_merge['bl_result_key']=bl_dataframe_max_merge.iloc[:,bl_tbl_size-3].astype(str) + "-" + bl_dataframe_max_merge.iloc[:,bl_tbl_size-2].astype(str) + "-" + bl_dataframe_max_merge.iloc[:,bl_tbl_size-1].astype(str)
        bl_dataframe_min_merge_task['bl_result_key_task']=bl_dataframe_min_merge_task.iloc[:,bl_tbl_size_task-1].astype(str)
    
        up_dataframe_min_merge=pd.merge(up_dataframe,bl_dataframe[['min_key','fixed_index']],left_on='min_key',right_on='min_key', how='left')
        up_dataframe_mid_merge=pd.merge(up_dataframe_min_merge,bl_dataframe[['mid_key','fixed_index']],left_on='mid_key',right_on='mid_key', how='left')  
        up_dataframe_max_merge=pd.merge(up_dataframe_mid_merge,bl_dataframe[['max_key','fixed_index']],left_on='max_key',right_on='max_key', how='left')
        
        up_dataframe_min_merge_task=pd.merge(up_dataframe_task,bl_dataframe_task[['min_key','fixed_index']],left_on='min_key',right_on='min_key', how='left')

        up_tbl_size=len(up_dataframe_max_merge.columns)
        up_tbl_size_task=len(up_dataframe_min_merge_task.columns)
        up_dataframe_max_merge=up_dataframe_max_merge.fillna('N')
        up_dataframe_min_merge_task=up_dataframe_min_merge_task.fillna('N')
        up_dataframe_max_merge['bl_result_key']=up_dataframe_max_merge.iloc[:,up_tbl_size-3].astype(str) + "-" + up_dataframe_max_merge.iloc[:,up_tbl_size-2].astype(str) + "-" + up_dataframe_max_merge.iloc[:,up_tbl_size-1].astype(str)
        up_dataframe_min_merge_task['up_result_key_task']=up_dataframe_min_merge_task.iloc[:,up_tbl_size_task-1].astype(str)

        bl_dataframe_max_merge['Change']=bl_dataframe_max_merge.iloc[:,bl_tbl_size].apply(lambda x: Bl_ApplyStatus(x))
        up_dataframe_max_merge['Change']=up_dataframe_max_merge.iloc[:,up_tbl_size].apply(lambda x: Up_ApplyStatus(x))
        

        bl_dataframe_min_merge_task['Change']=bl_dataframe_min_merge_task.iloc[:,bl_tbl_size_task].apply(lambda x: Bl_ApplyStatusTask(x))
        up_dataframe_min_merge_task['Change']=up_dataframe_min_merge_task.iloc[:,up_tbl_size_task].apply(lambda x: Up_ApplyStatusTask(x))


        export_name='Changes during-' + total_file_names[i+1] + '.xlsx'
        graph_name='Changes during-' + total_file_names[i+1]

        bl_export_scope=bl_dataframe_max_merge[bl_dataframe_max_merge['Change']!="NO CHANGE"]
        up_export_scope=up_dataframe_max_merge[up_dataframe_max_merge['Change']!="NO CHANGE"]

        bl_export_scope_task=bl_dataframe_min_merge_task[bl_dataframe_min_merge_task['Change']!="NO CHANGE"]
        up_export_scope_task=up_dataframe_min_merge_task[up_dataframe_min_merge_task['Change']!="NO CHANGE"]


        export_scope=bl_export_scope.append(up_export_scope)
        export_scope=export_scope[['Predecessor','Successor','pred_type','lag_hr_cnt','XER_FILE_NAME','Change']]
        export_scope['Change Period']=export_name
        export_scope_task=bl_export_scope_task.append(up_export_scope_task)
        export_scope_task=export_scope_task[['task_code','XER_FILE_NAME','Change']]
        export_scope_task['Change Period']=export_name

        export_scope_master=pd.concat([export_scope_master,export_scope])
        export_scope_task_master=pd.concat([export_scope_task_master,export_scope_task])



        export_scope['lag_hr_cnt']=pd.to_numeric(export_scope['lag_hr_cnt'], errors='coerce')
        export_scope['lag_hr_cnt']=export_scope['lag_hr_cnt'].div(daily_working_hrs).round(2)




        #bl_dataframe_max_merge.to_excel('bll.xlsx')
        #up_dataframe_max_merge.to_excel('upp.xlsx')

        GenerateGraph(export_scope,graph_name,export_scope_task)


    export_scope_master['lag_hr_cnt']=pd.to_numeric(export_scope_master['lag_hr_cnt'], errors='coerce')
    export_scope_master['lag_hr_cnt']=export_scope_master['lag_hr_cnt'].div(daily_working_hrs).round(2)

    writer = pd.ExcelWriter(my_path+'/Overall Changes.xlsx', engine='xlsxwriter')

    export_scope_master.to_excel(writer ,sheet_name="TASKPRED")
    export_scope_task_master.to_excel(writer ,sheet_name="TASK")
    writer.save()

    master_xer_evolution=master_xer_evolution[master_xer_evolution.iloc[:,0]!='task_code']

    master_xer_evolution['total_float_hr_cnt']=pd.to_numeric(master_xer_evolution['total_float_hr_cnt'], errors='coerce')

    master_xer_evolution['remain_drtn_hr_cnt']=pd.to_numeric(master_xer_evolution['remain_drtn_hr_cnt'], errors='coerce')
    master_xer_evolution['phys_complete_pct']=pd.to_numeric(master_xer_evolution['phys_complete_pct'], errors='coerce')
    master_xer_evolution['target_drtn_hr_cnt']=pd.to_numeric(master_xer_evolution['target_drtn_hr_cnt'], errors='coerce')




    master_xer_evolution['status_code']=master_xer_evolution['status_code'].apply(lambda x: ActStatus(x))



    master_xer_evolution['total_float_hr_cnt']=master_xer_evolution['total_float_hr_cnt'].div(daily_working_hrs).round(2)
    master_xer_evolution['remain_drtn_hr_cnt']=master_xer_evolution['remain_drtn_hr_cnt'].div(daily_working_hrs).round(2)
    master_xer_evolution['target_drtn_hr_cnt']=master_xer_evolution['target_drtn_hr_cnt'].div(daily_working_hrs).round(2)
    master_xer_evolution['phys_complete_pct']=master_xer_evolution['phys_complete_pct'].div(100).round(2)

    writer_evol = pd.ExcelWriter(my_path+'/TableofEvolution.xlsx', engine='xlsxwriter')

    total_float_df=master_xer_evolution.pivot(index='task_code',columns='XER_FILE_NAME', values='total_float_hr_cnt')
    phys_complete_pct_df=master_xer_evolution.pivot(index='task_code',columns='XER_FILE_NAME', values='phys_complete_pct')
    status_code_df=master_xer_evolution.pivot(index='task_code',columns='XER_FILE_NAME', values='status_code')
    remain_drtn_hr_cnt_df=master_xer_evolution.pivot(index='task_code',columns='XER_FILE_NAME', values='remain_drtn_hr_cnt')
    target_drtn_hr_cnt_df=master_xer_evolution.pivot(index='task_code',columns='XER_FILE_NAME', values='target_drtn_hr_cnt')
    start_df=master_xer_evolution.pivot(index='task_code',columns='XER_FILE_NAME', values='Start')
    finish_df=master_xer_evolution.pivot(index='task_code',columns='XER_FILE_NAME', values='Finish')


    master_xer_evolution.to_excel(my_path+'/Alldata.xlsx')
    #total_float_df.to_excel('Pivot.xlsx')

    total_float_df.to_excel(writer_evol ,sheet_name="TOTAL FLOAT")
    phys_complete_pct_df.to_excel(writer_evol ,sheet_name="PERCENT COMPLETE")
    status_code_df.to_excel(writer_evol ,sheet_name="STATUS")
    remain_drtn_hr_cnt_df.to_excel(writer_evol ,sheet_name="REM DUR")
    target_drtn_hr_cnt_df.to_excel(writer_evol ,sheet_name="ORG DUR")
    start_df.to_excel(writer_evol ,sheet_name="START")
    finish_df.to_excel(writer_evol ,sheet_name="FINISH")
    writer_evol.save()







