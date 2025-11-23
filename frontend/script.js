// frontend/script.js
document.addEventListener('DOMContentLoaded', () => {
    const jobList = document.getElementById('job-list');
    const statusFilter = document.getElementById('status-filter');
    const searchBox = document.getElementById('search-box');
    const jobCount = document.getElementById('job-count');

    let searchTimeout;
    // ? (If slow) Implement local database cache and add a button to refresh the cache

    const fetchAndRenderJobs = async () => {
        const status = statusFilter.value;
        const query = searchBox.value;

        // Build the API URL with query parameters
        const url = new URL('/api/jobs', window.location.origin);
        if (status && status !== 'all') {
            url.searchParams.append('status', status);
        }
        if (query) {
            url.searchParams.append('q', query);
        }

        try {
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const jobs = await response.json();
            renderJobs(jobs);
        } catch (error) {
            console.error("Failed to fetch jobs:", error);
            jobList.innerHTML = `<tr><td colspan="6" style="color: red; text-align: center;">Error loading jobs. Is the backend server running?</td></tr>`;
            jobCount.textContent = 'Could not load jobs.';
        }
    };

    const renderJobs = (jobs) => {
        jobList.innerHTML = ''; // Clear existing list

        if (jobs.length === 0) {
            jobList.innerHTML = `<tr><td colspan="6" style="text-align: center;">No jobs found.</td></tr>`;
            jobCount.textContent = '0 jobs found.';
            return;
        }

        jobCount.textContent = `${jobs.length} job(s) found.`;

        jobs.forEach(job => {
            const row = document.createElement('tr');
            
            // Status Badge
            const statusCell = document.createElement('td');
            const statusClass = `status-${job.status || 'default'}`;
            statusCell.innerHTML = `<span class="status-badge ${statusClass}">${job.status}</span>`;

            // Simple cell for other data
            const createCell = (text) => {
                const cell = document.createElement('td');
                cell.textContent = text || 'N/A'; // Handle empty values
                return cell;
            };

            // Actions Cell
            const actionsCell = document.createElement('td');
            // Link to the saved HTML file
            const viewLink = document.createElement('a');
            viewLink.href = `/jobs/${job.Filename}`; // Served by FastAPI's StaticFiles
            viewLink.textContent = 'View';
            viewLink.target = '_blank'; // Open in new tab
            viewLink.className = 'action-btn btn-view';
            actionsCell.appendChild(viewLink);
            
            // Add buttons to change status
            const nextStatus = (job.status === 'new') ? 'viewed' : 'new';
            const updateButton = document.createElement('button');
            updateButton.textContent = `Mark as ${nextStatus}`;
            updateButton.className = 'action-btn btn-update';
            updateButton.onclick = () => updateStatus(job.Filename, nextStatus);
            actionsCell.appendChild(updateButton);

            // Date added
            last_mod_date = new Date(job['last_mod_time'])
            const date_options = {
                weekday: "short",
                day: "numeric",
                month: "short",
                year: "numeric",
                hour: "2-digit",
                minute: "2-digit",
                timeZoneName: "short",
                // hourCycle: "h23",
            };
            job_added_date = last_mod_date.toLocaleDateString("en-DE", date_options)

            row.appendChild(statusCell);
            row.appendChild(actionsCell);
            row.appendChild(createCell(job['Job title']));
            row.appendChild(createCell(job['German language fluency required']));
            row.appendChild(createCell(job['Is tech job']));
            row.appendChild(createCell(job['Role seniority']));
            row.appendChild(createCell(job['Company name']));
            row.appendChild(createCell(job['Location']));
            row.appendChild(createCell(job_added_date));
            row.appendChild(createCell(job['Job category']));

            jobList.appendChild(row);
        });
    };

    const updateStatus = async (filename, newStatus) => {
        try {
            const response = await fetch(`/api/jobs/${filename}/status`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ status: newStatus }),
            });

            if (!response.ok) {
                throw new Error('Failed to update status');
            }
            // Refresh the list to show the change
            fetchAndRenderJobs();
        } catch (error) {
            console.error("Error updating status:", error);
            alert('Could not update job status.');
        }
    };

    // Event Listeners for filters
    statusFilter.addEventListener('change', fetchAndRenderJobs);
    searchBox.addEventListener('input', () => {
        clearTimeout(searchTimeout);
        // Debounce search input to avoid excessive API calls
        searchTimeout = setTimeout(fetchAndRenderJobs, 300);
    });

    // Initial load
    fetchAndRenderJobs();
});
