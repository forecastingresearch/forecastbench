
        $(function()
        {
            const data = [{'Rank': 1, 'Team': 'fri.png', 'Model Organization': 'OpenAI', 'Model': 'GPT-4.5-Preview-2025-02-27 (zero shot with freeze values)', 'Dataset': 0.158, 'N dataset': 1181, 'Market': 0.035, 'N market': 61, 'Overall': 0.097, 'N': 1242, '95% CI': '[0.087, 0.097]', 'P-value to best': '—', 'Pct times № 1': 0.0, 'Pct times top 5%': 50.0, 'x% oracle equiv': '68%', 'Peer': 0.188, 'BSS': 0.056, 'Model Organization Logo': 'openai.svg'}, {'Rank': 1, 'Team': 'fri.png', 'Model Organization': 'Anthropic', 'Model': 'Claude-3-7-Sonnet-20250219 (scratchpad with freeze values)', 'Dataset': 0.157, 'N dataset': 1181, 'Market': 0.038, 'N market': 61, 'Overall': 0.097, 'N': 1242, '95% CI': '[0.087, 0.096]', 'P-value to best': '1.00', 'Pct times № 1': 50.0, 'Pct times top 5%': 100.0, 'x% oracle equiv': '68%', 'Peer': 0.187, 'BSS': 0.055, 'Model Organization Logo': 'anthropic.svg'}, {'Rank': 3, 'Team': 'fri.png', 'Model Organization': 'OpenAI', 'Model': 'GPT-4.5-Preview-2025-02-27 (scratchpad with freeze values)', 'Dataset': 0.151, 'N dataset': 1181, 'Market': 0.047, 'N market': 61, 'Overall': 0.099, 'N': 1242, '95% CI': '[0.087, 0.096]', 'P-value to best': '0.50', 'Pct times № 1': 50.0, 'Pct times top 5%': 50.0, 'x% oracle equiv': '68%', 'Peer': 0.185, 'BSS': 0.054, 'Model Organization Logo': 'openai.svg'}, {'Rank': 4, 'Team': 'fri.png', 'Model Organization': 'Anthropic', 'Model': 'Claude-3-7-Sonnet-20250219 (zero shot with freeze values)', 'Dataset': 0.165, 'N dataset': 1181, 'Market': 0.038, 'N market': 61, 'Overall': 0.101, 'N': 1242, '95% CI': '[0.092, 0.098]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '68%', 'Peer': 0.183, 'BSS': 0.051, 'Model Organization Logo': 'anthropic.svg'}, {'Rank': 5, 'Team': 'fri.png', 'Model Organization': 'Anthropic', 'Model': 'Claude-3-5-Sonnet-20241022 (zero shot with freeze values)', 'Dataset': 0.168, 'N dataset': 1181, 'Market': 0.038, 'N market': 61, 'Overall': 0.103, 'N': 1242, '95% CI': '[0.092, 0.098]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '67%', 'Peer': 0.182, 'BSS': 0.05, 'Model Organization Logo': 'anthropic.svg'}, {'Rank': 6, 'Team': 'fri.png', 'Model Organization': 'OpenAI', 'Model': 'O3-Mini-2025-01-31 (zero shot with freeze values)', 'Dataset': 0.167, 'N dataset': 1181, 'Market': 0.041, 'N market': 61, 'Overall': 0.104, 'N': 1242, '95% CI': '[0.095, 0.101]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '67%', 'Peer': 0.181, 'BSS': 0.049, 'Model Organization Logo': 'openai.svg'}, {'Rank': 7, 'Team': 'fri.png', 'Model Organization': 'Anthropic', 'Model': 'Claude-3-5-Sonnet-20240620 (zero shot with freeze values)', 'Dataset': 0.173, 'N dataset': 1181, 'Market': 0.037, 'N market': 61, 'Overall': 0.105, 'N': 1242, '95% CI': '[0.095, 0.102]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '67%', 'Peer': 0.18, 'BSS': 0.048, 'Model Organization Logo': 'anthropic.svg'}, {'Rank': 7, 'Team': 'fri.png', 'Model Organization': 'Anthropic', 'Model': 'Claude-3-5-Sonnet-20241022 (scratchpad with freeze values)', 'Dataset': 0.174, 'N dataset': 1181, 'Market': 0.036, 'N market': 61, 'Overall': 0.105, 'N': 1242, '95% CI': '[0.095, 0.103]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '67%', 'Peer': 0.18, 'BSS': 0.048, 'Model Organization Logo': 'anthropic.svg'}, {'Rank': 7, 'Team': 'fri.png', 'Model Organization': 'DeepSeek', 'Model': 'DeepSeek-R1 (scratchpad with freeze values)', 'Dataset': 0.164, 'N dataset': 1181, 'Market': 0.047, 'N market': 61, 'Overall': 0.105, 'N': 1242, '95% CI': '[0.095, 0.103]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '67%', 'Peer': 0.179, 'BSS': 0.047, 'Model Organization Logo': 'deepseek.svg'}, {'Rank': 10, 'Team': 'fri.png', 'Model Organization': 'Anthropic', 'Model': 'Claude-3-5-Sonnet-20240620 (scratchpad with freeze values)', 'Dataset': 0.175, 'N dataset': 1181, 'Market': 0.037, 'N market': 61, 'Overall': 0.106, 'N': 1242, '95% CI': '[0.099, 0.102]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '67%', 'Peer': 0.179, 'BSS': 0.047, 'Model Organization Logo': 'anthropic.svg'}, {'Rank': 11, 'Team': 'fri.png', 'Model Organization': 'DeepSeek', 'Model': 'DeepSeek-V3 (zero shot with freeze values)', 'Dataset': 0.178, 'N dataset': 1181, 'Market': 0.048, 'N market': 61, 'Overall': 0.113, 'N': 1242, '95% CI': '[0.105, 0.107]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '66%', 'Peer': 0.172, 'BSS': 0.04, 'Model Organization Logo': 'deepseek.svg'}, {'Rank': 12, 'Team': 'fri.png', 'Model Organization': 'OpenAI', 'Model': 'GPT-4.5-Preview-2025-02-27 (zero shot)', 'Dataset': 0.158, 'N dataset': 1181, 'Market': 0.071, 'N market': 61, 'Overall': 0.114, 'N': 1242, '95% CI': '[0.092, 0.116]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '66%', 'Peer': 0.17, 'BSS': 0.038, 'Model Organization Logo': 'openai.svg'}, {'Rank': 13, 'Team': 'fri.png', 'Model Organization': 'Meta', 'Model': 'Llama-3.3-70B-Instruct-Turbo (scratchpad with freeze values)', 'Dataset': 0.181, 'N dataset': 1181, 'Market': 0.052, 'N market': 61, 'Overall': 0.116, 'N': 1242, '95% CI': '[0.109, 0.114]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '65%', 'Peer': 0.168, 'BSS': 0.036, 'Model Organization Logo': 'meta.svg'}, {'Rank': 14, 'Team': 'fri.png', 'Model Organization': 'DeepSeek', 'Model': 'DeepSeek-V3 (scratchpad with freeze values)', 'Dataset': 0.174, 'N dataset': 1181, 'Market': 0.06, 'N market': 61, 'Overall': 0.117, 'N': 1242, '95% CI': '[0.105, 0.113]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '65%', 'Peer': 0.168, 'BSS': 0.036, 'Model Organization Logo': 'deepseek.svg'}, {'Rank': 14, 'Team': 'fri.png', 'Model Organization': 'Anthropic', 'Model': 'Claude-3-7-Sonnet-20250219 (scratchpad)', 'Dataset': 0.157, 'N dataset': 1181, 'Market': 0.076, 'N market': 61, 'Overall': 0.117, 'N': 1242, '95% CI': '[0.098, 0.113]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '65%', 'Peer': 0.168, 'BSS': 0.036, 'Model Organization Logo': 'anthropic.svg'}, {'Rank': 16, 'Team': 'fri.png', 'Model Organization': 'OpenAI', 'Model': 'O3-Mini-2025-01-31 (scratchpad with freeze values)', 'Dataset': 0.17, 'N dataset': 1181, 'Market': 0.068, 'N market': 61, 'Overall': 0.119, 'N': 1242, '95% CI': '[0.11, 0.111]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '65%', 'Peer': 0.166, 'BSS': 0.034, 'Model Organization Logo': 'openai.svg'}, {'Rank': 16, 'Team': 'fri.png', 'Model Organization': 'OpenAI', 'Model': 'GPT-4.5-Preview-2025-02-27 (scratchpad)', 'Dataset': 0.151, 'N dataset': 1181, 'Market': 0.087, 'N market': 61, 'Overall': 0.119, 'N': 1242, '95% CI': '[0.107, 0.108]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '65%', 'Peer': 0.166, 'BSS': 0.034, 'Model Organization Logo': 'openai.svg'}, {'Rank': 18, 'Team': 'fri.png', 'Model Organization': 'Anthropic', 'Model': 'Claude-3-5-Sonnet-20241022 (zero shot)', 'Dataset': 0.168, 'N dataset': 1181, 'Market': 0.072, 'N market': 61, 'Overall': 0.12, 'N': 1242, '95% CI': '[0.098, 0.109]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '65%', 'Peer': 0.164, 'BSS': 0.033, 'Model Organization Logo': 'anthropic.svg'}, {'Rank': 18, 'Team': 'fri.png', 'Model Organization': 'Meta', 'Model': 'Llama-3.3-70B-Instruct-Turbo (zero shot with freeze values)', 'Dataset': 0.187, 'N dataset': 1181, 'Market': 0.053, 'N market': 61, 'Overall': 0.12, 'N': 1242, '95% CI': '[0.109, 0.114]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '65%', 'Peer': 0.165, 'BSS': 0.033, 'Model Organization Logo': 'meta.svg'}, {'Rank': 20, 'Team': 'fri.png', 'Model Organization': 'Anthropic', 'Model': 'Claude-3-7-Sonnet-20250219 (zero shot)', 'Dataset': 0.165, 'N dataset': 1181, 'Market': 0.079, 'N market': 61, 'Overall': 0.122, 'N': 1242, '95% CI': '[0.102, 0.112]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '65%', 'Peer': 0.162, 'BSS': 0.031, 'Model Organization Logo': 'anthropic.svg'}, {'Rank': 20, 'Team': 'fri.png', 'Model Organization': 'DeepSeek', 'Model': 'DeepSeek-R1 (scratchpad)', 'Dataset': 0.164, 'N dataset': 1181, 'Market': 0.08, 'N market': 61, 'Overall': 0.122, 'N': 1242, '95% CI': '[0.111, 0.117]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '65%', 'Peer': 0.163, 'BSS': 0.031, 'Model Organization Logo': 'deepseek.svg'}, {'Rank': 20, 'Team': 'fri.png', 'Model Organization': 'Anthropic', 'Model': 'Claude-3-5-Sonnet-20240620 (zero shot)', 'Dataset': 0.173, 'N dataset': 1181, 'Market': 0.072, 'N market': 61, 'Overall': 0.122, 'N': 1242, '95% CI': '[0.105, 0.115]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '65%', 'Peer': 0.162, 'BSS': 0.03, 'Model Organization Logo': 'anthropic.svg'}, {'Rank': 23, 'Team': 'fri.png', 'Model Organization': 'Anthropic', 'Model': 'Claude-3-5-Sonnet-20241022 (scratchpad)', 'Dataset': 0.174, 'N dataset': 1181, 'Market': 0.073, 'N market': 61, 'Overall': 0.123, 'N': 1242, '95% CI': '[0.102, 0.115]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '64%', 'Peer': 0.161, 'BSS': 0.029, 'Model Organization Logo': 'anthropic.svg'}, {'Rank': 24, 'Team': 'fri.png', 'Model Organization': 'DeepSeek', 'Model': 'DeepSeek-R1 (zero shot)', 'Dataset': 0.213, 'N dataset': 1181, 'Market': 0.042, 'N market': 61, 'Overall': 0.128, 'N': 1242, '95% CI': '[0.12, 0.124]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '64%', 'Peer': 0.157, 'BSS': 0.025, 'Model Organization Logo': 'deepseek.svg'}, {'Rank': 25, 'Team': 'fri.png', 'Model Organization': 'DeepSeek', 'Model': 'DeepSeek-R1 (zero shot with freeze values)', 'Dataset': 0.213, 'N dataset': 1181, 'Market': 0.048, 'N market': 61, 'Overall': 0.131, 'N': 1242, '95% CI': '[0.119, 0.132]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '63%', 'Peer': 0.154, 'BSS': 0.022, 'Model Organization Logo': 'deepseek.svg'}, {'Rank': 26, 'Team': 'fri.png', 'Model Organization': 'Anthropic', 'Model': 'Claude-3-5-Sonnet-20240620 (scratchpad)', 'Dataset': 0.175, 'N dataset': 1181, 'Market': 0.091, 'N market': 61, 'Overall': 0.133, 'N': 1242, '95% CI': '[0.123, 0.126]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '63%', 'Peer': 0.151, 'BSS': 0.02, 'Model Organization Logo': 'anthropic.svg'}, {'Rank': 27, 'Team': 'fri.png', 'Model Organization': 'OpenAI', 'Model': 'O3-Mini-2025-01-31 (zero shot)', 'Dataset': 0.167, 'N dataset': 1181, 'Market': 0.106, 'N market': 61, 'Overall': 0.137, 'N': 1242, '95% CI': '[0.128, 0.131]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '63%', 'Peer': 0.148, 'BSS': 0.016, 'Model Organization Logo': 'openai.svg'}, {'Rank': 28, 'Team': 'fri.png', 'Model Organization': 'Meta', 'Model': 'Llama-3.3-70B-Instruct-Turbo (zero shot)', 'Dataset': 0.187, 'N dataset': 1181, 'Market': 0.091, 'N market': 61, 'Overall': 0.139, 'N': 1242, '95% CI': '[0.129, 0.138]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '62%', 'Peer': 0.146, 'BSS': 0.014, 'Model Organization Logo': 'meta.svg'}, {'Rank': 29, 'Team': 'fri.png', 'Model Organization': 'DeepSeek', 'Model': 'DeepSeek-V3 (zero shot)', 'Dataset': 0.178, 'N dataset': 1181, 'Market': 0.107, 'N market': 61, 'Overall': 0.142, 'N': 1242, '95% CI': '[0.129, 0.138]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '62%', 'Peer': 0.142, 'BSS': 0.01, 'Model Organization Logo': 'deepseek.svg'}, {'Rank': 30, 'Team': 'fri.png', 'Model Organization': 'ForecastBench', 'Model': 'Imputed Forecaster', 'Dataset': 0.25, 'N dataset': 1181, 'Market': 0.041, 'N market': 61, 'Overall': 0.145, 'N': 1242, '95% CI': '[0.138, 0.148]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '61%', 'Peer': 0.139, 'BSS': 0.007, 'Model Organization Logo': 'fri.png'}, {'Rank': 31, 'Team': 'fri.png', 'Model Organization': 'Meta', 'Model': 'Llama-3.3-70B-Instruct-Turbo (scratchpad)', 'Dataset': 0.181, 'N dataset': 1181, 'Market': 0.121, 'N market': 61, 'Overall': 0.151, 'N': 1242, '95% CI': '[0.143, 0.145]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '61%', 'Peer': 0.134, 'BSS': 0.002, 'Model Organization Logo': 'meta.svg'}, {'Rank': 31, 'Team': 'fri.png', 'Model Organization': 'DeepSeek', 'Model': 'DeepSeek-V3 (scratchpad)', 'Dataset': 0.174, 'N dataset': 1181, 'Market': 0.127, 'N market': 61, 'Overall': 0.151, 'N': 1242, '95% CI': '[0.133, 0.147]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '61%', 'Peer': 0.134, 'BSS': 0.002, 'Model Organization Logo': 'deepseek.svg'}, {'Rank': 33, 'Team': 'fri.png', 'Model Organization': 'OpenAI', 'Model': 'O3-Mini-2025-01-31 (scratchpad)', 'Dataset': 0.17, 'N dataset': 1181, 'Market': 0.133, 'N market': 61, 'Overall': 0.152, 'N': 1242, '95% CI': '[0.136, 0.141]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '61%', 'Peer': 0.133, 'BSS': 0.001, 'Model Organization Logo': 'openai.svg'}, {'Rank': 34, 'Team': 'fri.png', 'Model Organization': 'ForecastBench', 'Model': 'Naive Forecaster', 'Dataset': 0.264, 'N dataset': 1181, 'Market': 0.041, 'N market': 61, 'Overall': 0.153, 'N': 1242, '95% CI': '[0.147, 0.158]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '60%', 'Peer': 0.132, 'BSS': 0.0, 'Model Organization Logo': 'fri.png'}, {'Rank': 35, 'Team': 'fri.png', 'Model Organization': 'ForecastBench', 'Model': 'Always 0.5', 'Dataset': 0.25, 'N dataset': 1181, 'Market': 0.25, 'N market': 61, 'Overall': 0.25, 'N': 1242, '95% CI': '[0.25, 0.25]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '50%', 'Peer': 0.035, 'BSS': -0.097, 'Model Organization Logo': 'fri.png'}, {'Rank': 36, 'Team': 'fri.png', 'Model Organization': 'ForecastBench', 'Model': 'Always 0', 'Dataset': 0.381, 'N dataset': 1181, 'Market': 0.131, 'N market': 61, 'Overall': 0.256, 'N': 1242, '95% CI': '[0.26, 0.263]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '49%', 'Peer': 0.028, 'BSS': -0.103, 'Model Organization Logo': 'fri.png'}, {'Rank': 37, 'Team': 'fri.png', 'Model Organization': 'ForecastBench', 'Model': 'Random Uniform', 'Dataset': 0.322, 'N dataset': 1181, 'Market': 0.352, 'N market': 61, 'Overall': 0.337, 'N': 1242, '95% CI': '[0.336, 0.345]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '41%', 'Peer': -0.053, 'BSS': -0.184, 'Model Organization Logo': 'fri.png'}, {'Rank': 38, 'Team': 'fri.png', 'Model Organization': 'ForecastBench', 'Model': 'Always 1', 'Dataset': 0.619, 'N dataset': 1181, 'Market': 0.869, 'N market': 61, 'Overall': 0.744, 'N': 1242, '95% CI': '[0.737, 0.74]', 'P-value to best': '<0.001', 'Pct times № 1': 0.0, 'Pct times top 5%': 0.0, 'x% oracle equiv': '13%', 'Peer': -0.459, 'BSS': -0.591, 'Model Organization Logo': 'fri.png'}];
            const cols = ["Rank", "Team", "Model Organization", "Model Organization Logo", "Model",
                          "Dataset", "N dataset",
                          "Market", "N market", "Overall", "N", "95% CI", "P-value to best",
                          "Pct times № 1", "Pct times top 5%", "x% oracle equiv",
                          "Peer", "BSS"];
            const columns = cols.map(name => {
                const col = { data: name, title: name };
                if (name === "Rank") {
                  col.className = 'dt-center';
                }
                if (name === "Team") {
                  col.className = 'dt-center';
                  col.render = d =>
                      d
                      ? `<img src="/assets/images/org_logos/${d}" alt="" style="height:20px">`
                      : '';
                }

                if (name === "Model Organization") {
                  col.title = "Org.";
                  col.className = 'dt-center';
                  col.render = (d, t, row) => {
                    if (t === 'display') {
                      return row['Model Organization Logo']
                        ? `<img src="/assets/images/org_logos/${row['Model Organization Logo']}"
                                alt="${d}" style="height:20px">`
                        : d;
                    }
                    return d; // Use text value for search/sort
                  };
                }

                if (["N dataset", "N market", "N", "Model Organization Logo"].includes(name)) {
                  col.visible = false;
                }

                if (name === "Dataset") {
                  col.title = "Dataset (N)";
                  col.render = (d, t, row) =>
                    t === "display"
                      ? parseFloat(d).toFixed(3) +
                        ' <span class="n-count">(' +
                        Number(row["N dataset"]).toLocaleString() +
                        ")</span>"
                      : d;
                  col.orderSequence = ["asc", "desc"];
                }

                if (name === "Market") {
                  col.title = "Market (N)";
                  col.render = (d, t, row) =>
                    t === "display"
                      ? parseFloat(d).toFixed(3) +
                        ' <span class="n-count">(' +
                        Number(row["N market"]).toLocaleString() +
                        ")</span>"
                      : d;
                  col.orderSequence = ["asc", "desc"];
                }

                if (name === "Overall") {
                  col.title = "Overall (N)";
                  col.render = (d, t, row) =>
                    t === "display"
                      ? parseFloat(d).toFixed(3) +
                        ' <span class="n-count">(' +
                        Number(row["N"]).toLocaleString() +
                        ")</span>"
                      : d;
                  col.orderSequence = ["asc", "desc"];
                }

                if (name === "P-value to best" || name === "x% oracle equiv") col.orderable = false;

                if (name === "Pct times № 1") {
                  col.render = (d, t) => (t === "display" ? Math.round(d) + "%" : d);
                  col.orderSequence = ["desc", "asc"];
                }

                if (name === "Pct times top 5%") {
                  col.render = (d, t) => (t === "display" ? Math.round(d) + "%" : d);
                  col.orderSequence = ["desc", "asc"];
                }

                if (name === "95% CI") col.orderable = false;

                if (name === "Peer" || name === "BSS") {
                  col.render = (d, t) => (t === "display" ? parseFloat(d).toFixed(3) : d);
                  col.orderSequence = ["desc", "asc"];
                }

                return col;
            });

            $('#leaderboard-table-full').html(`
               <table id="lb" class="display compact hover" style="width:100%">
               <thead>
                 <tr>
                   <th>Rank</th>
                   <th class="column-header-tooltip" data-tooltip="Team">Team</th>
                   <th class="column-header-tooltip" data-tooltip="Org.">Org.</th>
                   <th><!-- Model Organization Logo --></th>
                   <th class="column-header-tooltip" data-tooltip="Model">Model</th>
                   <th class="column-header-tooltip" data-tooltip="Dataset (N)">Dataset (N)</th>
                   <th><!-- N dataset --></th>
                   <th class="column-header-tooltip" data-tooltip="Market (N)">Market (N)</th>
                   <th><!-- N market --></th>
                   <th class="column-header-tooltip" data-tooltip="Overall (N)">Overall (N)</th>
                   <th><!-- N --></th>
                   <th class="column-header-tooltip" data-tooltip="95% CI">95% CI</th>
                   <th class="column-header-tooltip" data-tooltip="P-value to best">P-value to best</th>
                   <th class="column-header-tooltip" data-tooltip="Pct times № 1">Pct times № 1</th>
                   <th class="column-header-tooltip" data-tooltip="Pct times top 5%">Pct times top 5%</th>
                   <th class="column-header-tooltip" data-tooltip="x% oracle equiv">x% oracle equiv</th>
                   <th class="column-header-tooltip" data-tooltip="Peer">Peer</th>
                   <th class="column-header-tooltip" data-tooltip="BSS">BSS</th>
                 </tr>
               </thead>
               <tbody></tbody>
             </table>
             `);
             const table = $("#lb").DataTable({
               data: data,
               columns: columns,
               order: [[cols.indexOf("Overall"), "asc"]],
               pageLength:25,
               lengthMenu:[[10,25,50,100,-1],[10,25,50,100,"All"]],
               paging: true,
               info: true,
               dom:'<"top"lfr>t<"bottom"<"info-pagination-wrapper"ip>>',
               responsive: true,
               search: { regex: true, smart: true },
               createdRow: function(row, data, dataIndex) {
                 if (["Superforecaster median forecast", "Public median forecast"].includes(data.Model)) {
                   $(row).css('background-color', '#fdece8');
                 }
               },
               infoCallback: function(settings, start, end, max, total, pre) {
                   return pre + '<br>last updated 2025-09-10';
               }
           });
           // Initialize tooltips after table is created
           initializeTooltips();
        });
        // Tooltip content object (defined globally for access)
        const tooltipContent = {
          'Team': `The team that submitted forecasts.`,
          'Org.': `The organization that developed the model.`,
          'Model': `The name of the model that was used to generate the forecasts.`,
          'Dataset (N)': `Average difficulty-adjusted Brier score on dataset-sourced questions. Rescaled so that Always 0.5 has a score of 0.25. Lower scores are better.`,
          'Market (N)': `Average difficulty-adjusted Brier score on market-sourced questions. Rescaled so that Always 0.5 has a score of 0.25. Lower scores are better.`,
          'Overall (N)': `Average difficulty-adjusted Brier score across all questions. Rescaled so that the Always 0.5 forecaster has a score of 0.25. Lower scores are better.`,
          '95% CI': `Bootstrapped 95% confidence interval for the Overall score.`,
          'P-value to best': `One-sided p-value comparing each model to the top-ranked model based on 2 simulations, with<br>H₀: This model performs at least as well as the top-ranked model.<br>H₁: The top-ranked model outperforms this model.`,
          'Pct times № 1': `Percentage of 2 simulations in which this model was the best performer.`,
          'Pct times top 5%': `Percentage of 2 simulations in which this model ranked in the top 5%.`,
          'x% oracle equiv': `This model is most similar to a reference model that forecasts x% when the question resolves to 1 and (1-x)% when the question resolved to 0. x moves in increments of 1 from 0 - 100 inclusive. The 100% forecaster can be viewed as an oracle.`,
          'Peer': `Peer score relative to the average Brier score on each question. Higher scores are better.`,
          'BSS': `Brier Skill Score using the ForecastBench Naive Forecaster. Higher scores are better.`
        };
        