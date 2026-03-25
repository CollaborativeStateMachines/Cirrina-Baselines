
Vagrant.configure("2") do |config|
  nodes = {
    "redis_pub_sub" => {},
    "arbitrator" => {},
    "w0" => { "PHILOSOPHER_ID" => "0" },
    "w1" => { "PHILOSOPHER_ID" => "1" },
    "w2" => { "PHILOSOPHER_ID" => "2" },
    "w3" => { "PHILOSOPHER_ID" => "3" },
    "w4" => { "PHILOSOPHER_ID" => "4" },
    "w5" => { "PHILOSOPHER_ID" => "5" }
  }

  nodes.each do |name, env_vars|
    config.vm.provider "virtualbox" do |vb|
      vb.customize ["guestproperty", "set", :id, "/VirtualBox/GuestAdd/VBoxService/--timesync-interval", 10000]
      vb.customize ["guestproperty", "set", :id, "/VirtualBox/GuestAdd/VBoxService/--timesync-set-threshold", 0.2]
    end
    config.vm.define name do |node|
      node.vm.box = "generic/ubuntu2004"
      ip_suffix = case name
        when "arbitrator" then 10
        when "redis_pub_sub" then 9
        else 11 + name[1..-1].to_i
      end
      node.vm.network "private_network", ip: "192.168.56.#{ip_suffix}"
      node.vm.synced_folder ".", "/app"
      node.vm.provision "shell", inline: <<-SHELL
        apt-get update -qq && apt-get install -y docker.io linuxptp

        # Every node gets own state store
        docker rm -f redis || true
        docker run -d --name redis --network host redis:8.2.4-alpine

        if [ "#{name}" = "redis_pub_sub" ]; then
            exit 0
        fi

        # Every node gets its own placement
        docker rm -f placement || true
        docker run -d --name placement --network host daprio/placement:1.16.0 ./placement --port 50006

        # Sidecar
        docker rm -f #{name}-sidecar || true
        docker run -d \
          --name #{name}-sidecar \
          --network host \
          -v /app/config/redis/components-vagrant:/components \
          daprio/daprd:edge \
          ./daprd \
          --app-id #{name}-sidecar \
          --app-port 3000 \
          --resources-path /components \
          --placement-host-address localhost:50006 \
          --metrics-port 9091

        sleep 2

        # Application
        docker rm -f #{name} || true
        if [ "#{name}" = "arbitrator" ]; then
          docker run -d \
            --name #{name} \
            --network host \
            -e ROLE=arbitrator \
            -e NUMBER_OF_PHILOSOPHERS=6 \
            -e DAPR_HTTP_ENDPOINT=http://localhost:3500 \
            -e METRICS_DIRECTORY=/app/metrics \
            -e DAPR_GRPC_ENDPOINT=http://localhost:50001 \
            -v /app/diningPhilosophers/run/#{name}/metrics:/app/metrics \
            collaborativestatemachines/cirrina-baselines-diningPhilosophers:unstable
        else
          docker run -d \
            --name #{name} \
            --network host \
            -e PHILOSOPHER_ID=#{env_vars['PHILOSOPHER_ID']} \
            -e DAPR_HTTP_ENDPOINT=http://localhost:3500 \
            -e METRICS_DIRECTORY=/app/metrics \
            -e DAPR_GRPC_ENDPOINT=http://localhost:50001 \
            -v /app/diningPhilosophers/run/#{name}/metrics:/app/metrics \
            collaborativestatemachines/cirrina-baselines-diningPhilosophers:unstable
        fi

        nohup bash -c 'while true; do docker stats --no-stream --format "$(date +%s),{{.Name}},{{.CPUPerc}},{{.MemUsage}}" >> /app/diningPhilosophers/run/metrics_#{name}/docker_stats.csv; sleep 1; done' &
      SHELL
    end
  end
end
