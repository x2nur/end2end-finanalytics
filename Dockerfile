FROM public.ecr.aws/glue/aws-glue-libs:5

ARG HOME=/home/hadoop 
ARG ENTRYPNT=${HOME}/.local/bin/entrypoint.sh  

WORKDIR ${HOME}

# test
# COPY nvim-linux-x86_64.tar.gz .

USER hadoop 

RUN pip3.11 --no-cache-dir install --user \
	notebook==7.5.3 \ 
	basedpyright==1.38.1 \
	jupyter-client==8.8.0 \
	websocket-client==1.9.0 \ 
	requests==2.32.5 \ 
	pynvim==0.6.0 \
	ipykernel==7.2.0 && \
	curl -LO https://github.com/junegunn/fzf/releases/download/v0.68.0/fzf-0.68.0-linux_amd64.tar.gz && \
	mkdir -p ${HOME}/.local/bin && \
	tar -C ${HOME}/.local/bin -xzf fzf-0.68.0-linux_amd64.tar.gz && \
	rm fzf-0.68.0-linux_amd64.tar.gz && \
	curl -LO https://github.com/luals/lua-language-server/releases/download/3.17.1/lua-language-server-3.17.1-linux-x64.tar.gz && \
	mkdir -p ${HOME}/.local/opt/lua-server && \
	tar -C ${HOME}/.local/opt/lua-server -xzf lua-language-server-3.17.1-linux-x64.tar.gz && \
	rm lua-language-server-3.17.1-linux-x64.tar.gz && \
	curl -LO https://github.com/neovim/neovim/releases/latest/download/nvim-linux-x86_64.tar.gz && \
	mkdir -p ${HOME}/.local/opt && \
	tar -C ${HOME}/.local/opt -xzf nvim-linux-x86_64.tar.gz && \
	rm nvim-linux-x86_64.tar.gz 

ENV PATH=${HOME}/.local/bin:$HOME/.local/opt/nvim-linux-x86_64/bin:$HOME/.local/opt/lua-server/bin:$PATH

USER root 

COPY nvim/ ${HOME}/.config/nvim/

RUN chown -R hadoop:hadoop ${HOME}/.config/nvim && \
	chmod -R +wr ${HOME}/.config/nvim 

COPY entrypoint.sh ${ENTRYPNT}

RUN chown hadoop:hadoop $ENTRYPNT && \
	chmod +x $ENTRYPNT 

# Spark UI, jupyter notebook
EXPOSE 4040 8888 

USER hadoop

RUN nvim --headless "+Lazy! sync" +qa

CMD ["-c", "entrypoint.sh"]
