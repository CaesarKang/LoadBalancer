## This script is used to dispatch computation tasks among the hpc cluster
## by Caesar
## 15 Mar 2019

include(raw"\\unicorn3\CRI3\DeepSelect_Refresh\SelectRegressors_Code\dataGenV3.jl")
include(raw"\\unicorn3\CRI3\DeepSelect_Refresh\SelectRegressors_Code\ismember_CK.jl")
using HTTP, CSV, PyCall
push!(pyimport("sys")["path"], raw"\\unicorn3\CRI3\DeepSelect_Refresh\Variable_Selection_API")
@pyimport Email_Alert

### client_servers cluster
# global client_servers = [
# "crisifi-2hyt"]
global client_servers = [
"CRI-HPC02",
"CRI-HPC03",
"CRI-HPC04",
"CRI-HPC05",
"CRI-HPC06",
"CRI-HPC09",
"CRI-HPC10",
"CRI-HPC11",
"CRI-HPC12",
"CRI-HPC14",
"CRI-HPC15",
"CRI-HPC16",
"CRI-HPC17",
"CRI-HPC19",
"CRI-PC21",
"CRI-PC23",
"CRI-PC24",
"CRI-PC25",
"CRI-PC26",
"CRI-PC27",
"CRI-PC34",
"CRI-PC35",
"CRI-PC36",
"CRI-PC38",
"CRI-HPC07",
"CRI-HPC08",
"CRI-HPC13",
"CRI-HPC18",
"CRI-PC39"
]



function load_balancer_gen(task_chnl::RemoteChannel{Channel{Any}}, csvr_chnl::RemoteChannel{Channel{Any}}, urls::Array{String}, params_dict::Dict, task_keys::Array{String}, error_path::String, r::Dict=Dict())
    date = string(Dates.now())[1:10]
    counter = 0
    task_id = 0
    r = Dict()
    filename = deepcopy(params_dict[task_keys[2]])

    @sync while isready(task_chnl)
        @sync while isready(task_chnl)
            task_id = take!(task_chnl)
            if !isready(csvr_chnl)
                wait(csvr_chnl)
            end
            @async begin
                iHPC = take!(csvr_chnl)
                url1 = replace(urls[1], "iHPC", iHPC)
                task = deepcopy(task_id)

                #task_id_cp = deepcopy(task_id[1]) ## to make task_id become a local variable
                #srd = deepcopy(task_id[2])
                #params_dict = input_proc(params_dict,task_keys,task_id_cp, srd)

                task_id_cp = deepcopy(task_id)
                params_dict = input_proc(params_dict,filename,task_keys,task_id_cp)

                try
                    r["task_$task_id_cp"] = HTTP.request("POST", url1, [], JSON.json(params_dict))
                    r["task_$task_id_cp"] = JSON.parse(String(r["task_$task_id_cp"].body))
                    put!(csvr_chnl, r["task_$task_id_cp"]["host"])
                    if r["task_$task_id_cp"]["error"] == true
                        put!(task_chnl, task)
                        counter = error_alert(counter, error_path, date, task_id_cp, r)
                    end
                catch e
                    bt = catch_backtrace()
                    println("enter into catch")
                    put!(task_chnl, task)
                    msg = sprint(showerror, e, bt)
                    counter = error_alert(counter, error_path, date, task_id_cp, iHPC, msg)
                end
            end
            sleep(1)
        end
    end
    if counter >= 2 && isfile(error_path * "\\error_task-$date.txt")
        contents = "Reassigning tasks for errors occurred when running simulation!"
        Email_Alert.sendemail("[DS]Error_report", contents, "error_task-$date.txt")
    end
    return r
end


function check_pc_status(client_servers::Array{String}, test_url::String)

    csvr_chnl = RemoteChannel(()->Channel(100), 1)
    final_csvr_chnl = RemoteChannel(()->Channel(100), 1)

    for i in client_servers
        put!(csvr_chnl, i)
    end

    prepInput(ikeys, ivalues) = JSON.json(Dict(zip(ikeys, ivalues)))
    ikeys = ["x", "y"]
    ivalues = [1, 1]
    paras_d_sim = prepInput(ikeys, ivalues)

    while isready(csvr_chnl)
        iHPC = take!(csvr_chnl)
        try
            test_url = replace(test_url, "iHPC", iHPC)
            r1 = HTTP.request("POST", test_url, [], paras_d_sim)
            res1 = JSON.parse(String(r1.body))
            if !res1["error"]
                put!(final_csvr_chnl, iHPC)
            end
        catch e
            bt = catch_backtrace()
            msg = sprint(showerror, e, bt)
            print("Error on $iHPC:\n", msg)
        end
    end

    return final_csvr_chnl
end


# Sys.free_memory()/2^23
# Sys.CPU_CORES


# function input_proc(params_dict,task_keys,task_id_cp, srd)
#
#     params_dict[task_keys[1]] = task_id_cp
#     params_dict[task_keys[2]] = srd
#
#     return params_dict
# end

function input_proc(params_dict,filename,task_keys,task_id_cp)

    params_dict[task_keys[1]] = task_id_cp
    filename_new = replace(filename, r"data.csv", "data_$task_id_cp.csv")
    params_dict[task_keys[2]] = filename_new

    return params_dict
end

function error_alert(counter, error_path, date, task_id_cp, r)
    open(error_path * "\\error_task-$date.txt", "a+") do file
        write(file, string(Dates.now())*"\n")
        write(file, "Errors occured when running task $task_id_cp on Host: " * r["task_$task_id_cp"]["host"])
        write(file, r["task_$task_id_cp"]["msg"])
        write(file, "\n")
    end
    counter += 1
    if counter == 1 && isfile(error_path * "\\error_task-$date.txt")
        contents = "Errors occurred when task $task_id_cp"
        Email_Alert.sendemail("[DS]Error_report", contents, "error_task-$date.txt")
    end
    return counter
end

function error_alert(counter, error_path, date, task_id_cp, iHPC, msg)
    open(error_path * "\\error_task-$date.txt", "a+") do file
        write(file, string(Dates.now())*"\n")
        write(file, "Errors occured when running task $task_id_cp on Host: " * iHPC)
        write(file, msg)
        write(file, "\n")
    end
    counter += 1
    if counter == 1 && isfile(error_path * "\\error_task-$date.txt")
        contents = "Errors occurred when task $task_id_cp"
        Email_Alert.sendemail("[DS]Error_report", contents, "error_task-$date.txt")
    end
    return counter
end
