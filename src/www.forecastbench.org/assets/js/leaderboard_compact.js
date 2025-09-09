
        $(function()
        {
            const data = [{'Rank': 1, 'Model Organization': 'OpenAI', 'Model Organization Logo': 'openai.svg', 'Model': 'GPT-4.5-Preview-2025-02-27 (zero shot with freeze values)', 'Overall': 0.097}, {'Rank': 1, 'Model Organization': 'Anthropic', 'Model Organization Logo': 'anthropic.svg', 'Model': 'Claude-3-7-Sonnet-20250219 (scratchpad with freeze values)', 'Overall': 0.097}, {'Rank': 3, 'Model Organization': 'OpenAI', 'Model Organization Logo': 'openai.svg', 'Model': 'GPT-4.5-Preview-2025-02-27 (scratchpad with freeze values)', 'Overall': 0.099}, {'Rank': 4, 'Model Organization': 'Anthropic', 'Model Organization Logo': 'anthropic.svg', 'Model': 'Claude-3-7-Sonnet-20250219 (zero shot with freeze values)', 'Overall': 0.101}, {'Rank': 5, 'Model Organization': 'Anthropic', 'Model Organization Logo': 'anthropic.svg', 'Model': 'Claude-3-5-Sonnet-20241022 (zero shot with freeze values)', 'Overall': 0.103}, {'Rank': 6, 'Model Organization': 'OpenAI', 'Model Organization Logo': 'openai.svg', 'Model': 'O3-Mini-2025-01-31 (zero shot with freeze values)', 'Overall': 0.104}, {'Rank': 7, 'Model Organization': 'Anthropic', 'Model Organization Logo': 'anthropic.svg', 'Model': 'Claude-3-5-Sonnet-20240620 (zero shot with freeze values)', 'Overall': 0.105}, {'Rank': 7, 'Model Organization': 'Anthropic', 'Model Organization Logo': 'anthropic.svg', 'Model': 'Claude-3-5-Sonnet-20241022 (scratchpad with freeze values)', 'Overall': 0.105}, {'Rank': 7, 'Model Organization': 'DeepSeek', 'Model Organization Logo': 'deepseek.svg', 'Model': 'DeepSeek-R1 (scratchpad with freeze values)', 'Overall': 0.105}, {'Rank': 10, 'Model Organization': 'Anthropic', 'Model Organization Logo': 'anthropic.svg', 'Model': 'Claude-3-5-Sonnet-20240620 (scratchpad with freeze values)', 'Overall': 0.106}, {'Rank': 11, 'Model Organization': 'DeepSeek', 'Model Organization Logo': 'deepseek.svg', 'Model': 'DeepSeek-V3 (zero shot with freeze values)', 'Overall': 0.113}, {'Rank': 12, 'Model Organization': 'OpenAI', 'Model Organization Logo': 'openai.svg', 'Model': 'GPT-4.5-Preview-2025-02-27 (zero shot)', 'Overall': 0.114}, {'Rank': 13, 'Model Organization': 'Meta', 'Model Organization Logo': 'meta.svg', 'Model': 'Llama-3.3-70B-Instruct-Turbo (scratchpad with freeze values)', 'Overall': 0.116}, {'Rank': 14, 'Model Organization': 'DeepSeek', 'Model Organization Logo': 'deepseek.svg', 'Model': 'DeepSeek-V3 (scratchpad with freeze values)', 'Overall': 0.117}, {'Rank': 14, 'Model Organization': 'Anthropic', 'Model Organization Logo': 'anthropic.svg', 'Model': 'Claude-3-7-Sonnet-20250219 (scratchpad)', 'Overall': 0.117}, {'Rank': 16, 'Model Organization': 'OpenAI', 'Model Organization Logo': 'openai.svg', 'Model': 'O3-Mini-2025-01-31 (scratchpad with freeze values)', 'Overall': 0.119}, {'Rank': 16, 'Model Organization': 'OpenAI', 'Model Organization Logo': 'openai.svg', 'Model': 'GPT-4.5-Preview-2025-02-27 (scratchpad)', 'Overall': 0.119}, {'Rank': 18, 'Model Organization': 'Anthropic', 'Model Organization Logo': 'anthropic.svg', 'Model': 'Claude-3-5-Sonnet-20241022 (zero shot)', 'Overall': 0.12}, {'Rank': 18, 'Model Organization': 'Meta', 'Model Organization Logo': 'meta.svg', 'Model': 'Llama-3.3-70B-Instruct-Turbo (zero shot with freeze values)', 'Overall': 0.12}, {'Rank': 20, 'Model Organization': 'Anthropic', 'Model Organization Logo': 'anthropic.svg', 'Model': 'Claude-3-7-Sonnet-20250219 (zero shot)', 'Overall': 0.122}, {'Rank': 20, 'Model Organization': 'DeepSeek', 'Model Organization Logo': 'deepseek.svg', 'Model': 'DeepSeek-R1 (scratchpad)', 'Overall': 0.122}, {'Rank': 20, 'Model Organization': 'Anthropic', 'Model Organization Logo': 'anthropic.svg', 'Model': 'Claude-3-5-Sonnet-20240620 (zero shot)', 'Overall': 0.122}, {'Rank': 23, 'Model Organization': 'Anthropic', 'Model Organization Logo': 'anthropic.svg', 'Model': 'Claude-3-5-Sonnet-20241022 (scratchpad)', 'Overall': 0.123}, {'Rank': 24, 'Model Organization': 'DeepSeek', 'Model Organization Logo': 'deepseek.svg', 'Model': 'DeepSeek-R1 (zero shot)', 'Overall': 0.128}, {'Rank': 25, 'Model Organization': 'DeepSeek', 'Model Organization Logo': 'deepseek.svg', 'Model': 'DeepSeek-R1 (zero shot with freeze values)', 'Overall': 0.131}, {'Rank': 26, 'Model Organization': 'Anthropic', 'Model Organization Logo': 'anthropic.svg', 'Model': 'Claude-3-5-Sonnet-20240620 (scratchpad)', 'Overall': 0.133}, {'Rank': 27, 'Model Organization': 'OpenAI', 'Model Organization Logo': 'openai.svg', 'Model': 'O3-Mini-2025-01-31 (zero shot)', 'Overall': 0.137}, {'Rank': 28, 'Model Organization': 'Meta', 'Model Organization Logo': 'meta.svg', 'Model': 'Llama-3.3-70B-Instruct-Turbo (zero shot)', 'Overall': 0.139}, {'Rank': 29, 'Model Organization': 'DeepSeek', 'Model Organization Logo': 'deepseek.svg', 'Model': 'DeepSeek-V3 (zero shot)', 'Overall': 0.142}, {'Rank': 30, 'Model Organization': 'ForecastBench', 'Model Organization Logo': 'fri.png', 'Model': 'Imputed Forecaster', 'Overall': 0.145}, {'Rank': 31, 'Model Organization': 'Meta', 'Model Organization Logo': 'meta.svg', 'Model': 'Llama-3.3-70B-Instruct-Turbo (scratchpad)', 'Overall': 0.151}, {'Rank': 31, 'Model Organization': 'DeepSeek', 'Model Organization Logo': 'deepseek.svg', 'Model': 'DeepSeek-V3 (scratchpad)', 'Overall': 0.151}, {'Rank': 33, 'Model Organization': 'OpenAI', 'Model Organization Logo': 'openai.svg', 'Model': 'O3-Mini-2025-01-31 (scratchpad)', 'Overall': 0.152}, {'Rank': 34, 'Model Organization': 'ForecastBench', 'Model Organization Logo': 'fri.png', 'Model': 'Naive Forecaster', 'Overall': 0.153}, {'Rank': 35, 'Model Organization': 'ForecastBench', 'Model Organization Logo': 'fri.png', 'Model': 'Always 0.5', 'Overall': 0.25}, {'Rank': 36, 'Model Organization': 'ForecastBench', 'Model Organization Logo': 'fri.png', 'Model': 'Always 0', 'Overall': 0.256}, {'Rank': 37, 'Model Organization': 'ForecastBench', 'Model Organization Logo': 'fri.png', 'Model': 'Random Uniform', 'Overall': 0.337}, {'Rank': 38, 'Model Organization': 'ForecastBench', 'Model Organization Logo': 'fri.png', 'Model': 'Always 1', 'Overall': 0.744}];
            $('#leaderboard-table').html(`
            <table id="lb" class="display stripe hover" style="width:100%">
              <thead>
                <tr>
                  <th>Rank</th>
                  <th class="column-header-tooltip" data-tooltip="Model Organization">Org.</th>
                  <th class="column-header-tooltip" data-tooltip="Model">Model</th>
                  <th class="column-header-tooltip" data-tooltip="Overall">Overall</th>
                </tr>
              </thead>
            </table>
            `);
            const table = $('#lb').DataTable({
              data:data,
              columns:[
                {data:'Rank', className: 'dt-center'},
                {
                  data:'Model Organization',
                  className: 'dt-center',
                  render: (d, type, row) => {
                    if (type === 'display') {
                      return row['Model Organization Logo']
                        ? `<img src="/assets/images/org_logos/${row['Model Organization Logo']}"
                                alt="${d}" style="height:20px">`
                        : d;
                    }
                    return d; // Use text value for search/sort
                  }
                },
                {data:'Model'},
                {data:'Overall',render:d=>parseFloat(d).toFixed(3)}
              ],
              order:[[3,'asc']],
              pageLength:10,
              lengthMenu:[[10,25,50,100,-1],[10,25,50,100,"All"]],
              paging:true,
              info:true,
              dom:'<"top"lfr>t<"bottom"<"info-pagination-wrapper"ip>>',
              responsive:true,
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
          'Organization': `The organization that developed the model.`,
          'Model': `The name of the model that was used to generate the forecasts.`,
          'Overall': `Average difficulty-adjusted Brier score across all questions. Rescaled so that the Always 0.5 forecaster has a score of 0.25. Lower scores are better.`
        };
        